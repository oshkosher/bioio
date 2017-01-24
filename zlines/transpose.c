/*
  Do a bytewise tranpose of a text file.

  For example, given this input:
    RGk
    ebU
    LwP
    Bkl
    Jd7
  produce this output:
    ReLBJ
    Gbwkd
    kUPl7

  The goal of this is to support arbitrarily large data sets (much larger than
  memory) efficiently.

  Ed Karrels, edk@illinois.edu, January 2017
*/

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include "common.h"

typedef uint64_t u64;


typedef struct {
  char *data;
  int n_cols, n_rows, row_stride;
} Array2d;

Array2d in, out;

/* Get a pointer to a character in an Array2d. 'a' should be an Array2d,
   not a pointer to one. */
#define Array2d_ptr(a, row, col) ((a).data + (u64)(row) * (a).row_stride + (col))

/* If nonzero, blocks read will progress to the right, then down, and
   blocks written will progress down, then right.
   If zero, the the opposite. */
int move_right = 0;

/* The data is processed in blocks. This is the number of rows in 
   each block read and number of columns in each block written. */
int read_block_height = 256;

/* Number of columns in each block read and number of rows
   in each block written. */
int read_block_width = 256;

int cache_ob_size = 256;

#define NEWLINE_UNIX 1
#define NEWLINE_DOS 2

int newline_type;

#define MIN(a,b) ((a)<(b)?(a):(b))
#define MAX(a,b) ((a)>(b)?(a):(b))

void printHelp();
int getFileDimensions(Array2d *array, u64 length, int *newline_type);
#define newlineLength(newline_type) (newline_type)
#define newlineName(newline_type) ((newline_type)==(NEWLINE_DOS)?"DOS":"unix")
void writeNewline(char *dest, int newline_type);

/* transpose the data using a block algorithm. */
void transposeBlocks();

/* transpose the data using a cache-oblivious algorithm. */
void transposeCacheOblivious();

/* used by transposeCacheOblivious() */
void transposeCacheObliviousRecurse(int block_top, int block_left,
                                    int block_height, int block_width);

/* Transpose one tile using a simple algorithm. */
void transposeTile(int block_left, int block_right,
                   int block_top, int block_bottom);



int main(int argc, char **argv) {
  int result = 0;
  int newline_len;
  u64 in_file_len, out_file_len = 0, byte_count;
  double start_time, elapsed, mbps;

  if (argc != 3) printHelp();
  
  if (mapFile(argv[1], 0, (char**)&in.data, &in_file_len)) {
    fprintf(stderr, "Failed to open %s\n", argv[1]);
    return 1;
  }

  if (getFileDimensions(&in, in_file_len, &newline_type))
    goto fail;

  newline_len = newlineLength(newline_type);
  out.n_cols = in.n_rows;
  out.n_rows = in.n_cols;
  in.row_stride = in.n_cols + newline_len;
  out.row_stride = out.n_cols + newline_len;

  printf("%s has %d rows of length %d with %s line endings\n",
         argv[1], in.n_rows, in.n_cols, newlineName(newline_type));

  out_file_len = (u64)out.n_rows * out.row_stride;

  if (mapFile(argv[2], 1, &out.data, &out_file_len)) {
    fprintf(stderr, "Failed to open %s\n", argv[2]);
    return 1;
  }

  start_time = getSeconds();
  
  /* transposeBlocks(&opt); */
  transposeCacheOblivious();

  putchar('\n');
  
  byte_count = (u64)in.n_rows * in.n_cols;
  elapsed = getSeconds() - start_time;
  mbps = byte_count / (elapsed * 1024 * 1024);
  printf("transpose %dx%d = %" PRIu64" bytes in %.3fs at %.1f MiB/s\n",
         in.n_rows, in.n_cols, byte_count, elapsed, mbps);


 fail:
  if (in.data) munmap(in.data, in_file_len);
  if (out.data) munmap(out.data, out_file_len);
  
  return result;
}


void printHelp() {
  printf("\n  transpose <input_file> <output_file>\n"
         "  Do a bytewise transpose of the lines of the given file.\n"
         "  Every line in the file must be the same length.\n\n");
  exit(1);
}


int getFileDimensions(Array2d *array, u64 length, int *newline_type) {
  const char *data, *first_newline;
  u64 row;

  data = array->data;
  
  first_newline = strchr(data, '\n');
  if (!first_newline) {
    fprintf(stderr, "Invalid input file: no line endings found\n");
    return -1;
  }

  if (first_newline == data) {
    fprintf(stderr, "Invalid input file: first line is empty\n");
    return -1;
  }

  if (first_newline[-1] == '\r') {
    *newline_type = NEWLINE_DOS;
  } else {
    *newline_type = NEWLINE_UNIX;
  }
  array->row_stride = (first_newline - data) + 1;
  array->n_cols = array->row_stride - newlineLength(*newline_type);

  array->n_rows = length / array->row_stride;
  if (length != array->n_rows * (u64)array->row_stride) {
    fprintf(stderr, "Invalid input file: uneven line lengths "
            "(rows appear to be %d bytes each, but that doesn't evenly "
            "divide the file length, %" PRIu64 "\n",
            array->row_stride, length);
    return -1;
  }

  /* check a few more rows */

  for (row=10; row < array->n_rows; row *= 10) {
    /* printf("check row %d\n", (int)row); */
    if (data[row * array->row_stride - 1] != '\n') {
      fprintf(stderr, "Invalid input file: row %d length mismatch\n", (int)row);
      return -1;
    }
  }

  return 0;
}


void writeNewline(char *dest, int newline_type) {
  if (newline_type == NEWLINE_DOS) {
    dest[0] = '\r';
    dest[1] = '\n';
  } else {
    dest[0] = '\n';
  }
}


void transposeTile(int block_left, int block_right,
                   int block_top, int block_bottom) {
  int x, y;
  static u64 bytes_done = 0, next_report = 100*1000*1000;

  /*
  printf("transpose (%d,%d) - (%d,%d)\n", block_top, block_left,
         block_bottom, block_right);
  */
  
  for (x = block_left; x < block_right; x++) {
    for (y = block_top; y < block_bottom; y++) {

      *Array2d_ptr(out, x, y) = *Array2d_ptr(in, y, x);

    }

    if (block_bottom == out.n_cols) {
      writeNewline(Array2d_ptr(out, x, out.n_cols), newline_type);
    }
  }

  bytes_done += (u64) (block_right - block_left) * (block_bottom - block_top);
  if (bytes_done > next_report) {
    char buf1[50], buf2[50];
    printf("\r%s of %s bytes done", commafy(buf1, bytes_done), 
           commafy(buf2, (u64)in.n_rows * in.n_cols));
    fflush(stdout);
    next_report += 100*1000*1000;
  }
}  


void transposeBlocks() {

  int block_left, block_top, block_right, block_bottom;

  for (block_left = 0; block_left < in.n_cols;
       block_left += read_block_width) {

    block_right = MIN(block_left + read_block_width, in.n_cols);

    for (block_top = 0; block_top < in.n_rows;
         block_top += read_block_height) {

      block_bottom = MIN(block_top + read_block_height, in.n_rows);

      transposeTile(block_left, block_right, block_top, block_bottom);

    }

    printf("\r%d rows written", block_right);
    fflush(stdout);
  }
  putchar('\n');
}


void transposeCacheOblivious() {
  transposeCacheObliviousRecurse(0, 0, in.n_rows, in.n_cols);
}

void transposeCacheObliviousRecurse(int block_top, int block_left,
                                    int block_height, int block_width) {

  if (block_height > cache_ob_size || block_width > cache_ob_size) {
    int half;
    if (block_height > block_width) {
      half = block_height / 2;
      transposeCacheObliviousRecurse(block_top, block_left,
                                     half, block_width);
      transposeCacheObliviousRecurse(block_top+half, block_left,
                                     block_height-half, block_width);
    } else {
      half = block_width / 2;
      transposeCacheObliviousRecurse(block_top, block_left,
                                     block_height, half);
      transposeCacheObliviousRecurse(block_top, block_left+half,
                                     block_height, block_width-half);
    }
    return;
  }      

  transposeTile(block_left, block_left + block_width,
                block_top, block_top + block_height);
}

                
