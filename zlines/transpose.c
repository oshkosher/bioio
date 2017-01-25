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

  Performance snapshot, 2015-01-25:
% ./transpose run1.chars /data/bioio/run1.chars.t
run1.chars has 1999999 rows of length 21946 with unix line endings
temp buffer 4096x4096
43,800,008,320 of 43,891,978,054 bytes done
transpose 1999999x21946 = 43891978054 bytes in 1084.340s at 38.6 MiB/s

  source drive: SSD, destination hard: HDD


  Ed Karrels, edk@illinois.edu, January 2017
*/

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include "common.h"

typedef uint64_t u64;


typedef struct {
  char *data;
  int n_rows, n_cols, row_stride;
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

int cache_ob_size = 128;

int default_temp_width = 4096;
int default_temp_height = 4096;

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
int createTempBuffer(Array2d *temp);

/* Transpose a 2d array. This will call one of the other transpose routines,
   depending on the size of the array. */
void transpose
(Array2d *dest, int dest_row, int dest_col,
 Array2d *src, int src_row, int src_col,
 int height, int width);

/* Transpose large chunks from the input file into memory, then copy them
   to the output file. */
void transposeTwoStage
(Array2d *dest, int dest_row, int dest_col,
 Array2d *src, int src_row, int src_col,
 int height, int width);

/* transpose the data using a block algorithm. */
void transposeBlocks
(Array2d *dest, int dest_row, int dest_col,
 Array2d *src, int src_row, int src_col,
 int height, int width);

/* transpose the data using a cache-oblivious algorithm. */
void transposeCacheOblivious
(Array2d *dest, int dest_row, int dest_col,
 Array2d *src, int src_row, int src_col,
 int height, int width);

/* Transpose one tile using a simple algorithm. */
void transposeTile
(Array2d *dest, int dest_row, int dest_col,
 Array2d *src, int src_row, int src_col,
 int height, int width);

/* simple copy without transposing */
void copy2d
(Array2d *dest, int dest_row, int dest_col,
 Array2d *src, int src_row, int src_col,
 int height, int width);


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

  byte_count = (u64)in.n_rows * in.n_cols;

  transposeTwoStage(&out, 0, 0, &in, 0, 0, in.n_rows, in.n_cols);

  putchar('\n');
  
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
  
  /* find the end of the first line of the file */
  first_newline = strchr(data, '\n');
  if (!first_newline) {
    fprintf(stderr, "Invalid input file: no line endings found\n");
    return -1;
  }

  if (first_newline == data) {
    fprintf(stderr, "Invalid input file: first line is empty\n");
    return -1;
  }

  /* check for DOS (0x0d 0x0a) line endings, since having two bytes at the
     end of each line will change the row stride of the data */
  if (first_newline[-1] == '\r') {
    *newline_type = NEWLINE_DOS;
  } else {
    *newline_type = NEWLINE_UNIX;
  }
  array->row_stride = (first_newline - data) + 1;
  array->n_cols = array->row_stride - newlineLength(*newline_type);

  /* For this tool to work correctly, every line of the file must have
     the same length. Don't check every line, but check that the file
     size is an integer multiple of the line length. */

  array->n_rows = length / array->row_stride;
  if (length != array->n_rows * (u64)array->row_stride) {
    fprintf(stderr, "Invalid input file: uneven line lengths "
            "(rows appear to be %d bytes each, but that doesn't evenly "
            "divide the file length, %" PRIu64 "\n",
            array->row_stride, length);
    return -1;
  }

  /* Check for the presence of a newline character in the right place
     in a few more rows. */

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


void writeTileNewlines(int block_top, int block_bottom) {
  int row;
  for (row = block_top; row < block_bottom; row++)
    writeNewline(Array2d_ptr(out, row, out.n_cols), newline_type);
}


int createTempBuffer(Array2d *temp) {
  u64 temp_size = (u64)default_temp_width * default_temp_height;

  assert(in.n_cols > 0 && in.n_rows > 0);

  if (in.n_cols < default_temp_height) {
    temp->n_rows = in.n_cols;
    temp->n_cols = MIN(in.n_rows, temp_size / in.n_cols);
  } else if (in.n_rows < default_temp_width) {
    temp->n_cols = in.n_rows;
    temp->n_rows = MIN(in.n_cols, temp_size / in.n_rows);
  } else {
    temp->n_rows = default_temp_height;
    temp->n_cols = default_temp_width;
  }

  printf("temp buffer %dx%d\n", temp->n_rows, temp->n_cols);
  temp->row_stride = temp->n_cols;
  
  temp->data = (char*) malloc((u64)temp->n_cols * temp->n_rows);
  if (!temp->data) {
    fprintf(stderr, "Failed to allocate %dx%d temp buffer.\n",
            temp->n_rows, temp->n_cols);
    return 1;
  }

  return 0;
}


/* Transpose a 2d array. This will call one of the other transpose routines,
   depending on the size of the array. */
void transpose
(Array2d *dest, int dest_row, int dest_col,
 Array2d *src, int src_row, int src_col,
 int height, int width) {

  if (height <= read_block_height &&
      width <= read_block_width) {
    transposeTile(dest, dest_row, dest_col, src, src_row, src_col,
                  height, width);
  } else {

    u64 byte_count = (u64) height * width;

    if (byte_count < 100*1024*1024) {

      /* choose either by blocks or with the cache-oblivious algorithm */
      
      transposeCacheOblivious
        /* transposeBlocks */
        (dest, dest_row, dest_col, src, src_row, src_col,
         height, width);

    } else {
      /* For really big arrays, first transpose large chunks from the
         input file into memory, then copy them to the output file. */
      transposeTwoStage(dest, dest_row, dest_col, src, src_row, src_col,
                        height, width);
    }
  }
}


/* Transpose large chunks from the input file into memory, then copy them
   to the output file. */
void transposeTwoStage
(Array2d *dest, int dest_row, int dest_col,
 Array2d *src, int src_row, int src_col,
 int height, int width) {
  Array2d temp = {0};

  if (createTempBuffer(&temp)) exit(1);

  int x, y, block_width, block_height;
  
  for (x = 0; x < width; x += temp.n_cols) {
    block_width = MIN(temp.n_cols, width - x);

    for (y = 0; y < height; y += temp.n_rows) {
      block_height = MIN(temp.n_rows, height - y);
      /*
      printf("transpose big tile (%d,%d) - (%d,%d)\n",
             src_row + y, src_col + x,
             src_row + y + block_height, src_col + x + block_width);
      */
      /* copy from the input file to temp, transposing along the way */
      transposeCacheOblivious
        (&temp, 0, 0,
         src, src_row + y, src_col + x,
         block_height, block_width);
      /*
      printf("write big tile to (%d,%d) - (%d,%d)\n",
             dest_row + x, dest_col + y,
             dest_row + x + block_width, dest_col + y + block_height);
      */
      /* copy from temp to the output file */

      copy2d(&out, dest_row + x, dest_col + y,
             &temp, 0, 0,
             block_width, block_height);


    }
  }
  

  free(temp.data);
}


void transposeTile(Array2d *dest, int dest_row, int dest_col,
                   Array2d *src, int src_row, int src_col,
                   int height, int width) {

  int x, y;
  static u64 bytes_done = 0, next_report = 100*1000*1000;

  /*
  printf("transpose (%d,%d) - (%d,%d)\n", src_row, src_col,
         src_row + height, src_col + width);
  */
  
  for (x = 0; x < width; x++) {
    for (y = 0; y < height; y++) {

      *Array2d_ptr(*dest, dest_row + x, dest_col + y) =
        *Array2d_ptr(*src, src_row + y, src_col + x);

    }

    /*
    if (block_bottom == out.n_cols) {
      writeNewline(Array2d_ptr(out, x, out.n_cols), newline_type);
    }
    */
  }

  bytes_done += (u64)width * height;
  if (bytes_done > next_report) {
    char buf1[50], buf2[50];
    printf("\r%s of %s bytes done", commafy(buf1, bytes_done), 
           commafy(buf2, (u64)in.n_rows * in.n_cols));
    fflush(stdout);
    next_report += 100*1000*1000;
  }
}  


/* height and width are in terms of the src array */
void transposeBlocks(Array2d *dest, int dest_row, int dest_col,
                     Array2d *src, int src_row, int src_col,
                     int height, int width) {

  int x, y, block_width, block_height;
  
  for (x = 0; x < width; x += read_block_width) {
    block_width = MIN(read_block_width, width - x);

    for (y = 0; y < height; y += read_block_height) {
      block_height = MIN(read_block_height, height - y);

      transposeTile(dest, dest_row + x, dest_col + y,
                    src, dest_row + y, src_col + x,
                    block_height, block_width);

      if (src_row + height == in.n_rows)
        writeTileNewlines(src_col, src_col + width);

    }
  }

}


void transposeCacheOblivious(Array2d *dest, int dest_row, int dest_col,
                             Array2d *src, int src_row, int src_col,
                             int height, int width) {

  if (height > cache_ob_size || width > cache_ob_size) {
    int half;
    if (height > width) {
      half = height / 2;
      transposeCacheOblivious(dest, dest_row, dest_col,
                              src, src_row, src_col,
                              half, width);
      transposeCacheOblivious(dest, dest_row, dest_col+half,
                              src, src_row+half, src_col,
                              height-half, width);
    } else {
      half = width / 2;
      transposeCacheOblivious(dest, dest_row, dest_col,
                              src, src_row, src_col,
                              height, half);
      transposeCacheOblivious(dest, dest_row+half, dest_col,
                              src, src_row, src_col+half,
                              height, width-half);
    }
    return;
  }      

  transposeTile(dest, dest_row, dest_col,
                src, src_row, src_col, height, width);

  if (src_row + height == in.n_rows)
    writeTileNewlines(src_col, src_col + width);

}

                
/* simple copy without transposing */
void copy2d
(Array2d *dest, int dest_row, int dest_col,
 Array2d *src, int src_row, int src_col,
 int height, int width) {

  int row;

  for (row = 0; row < height; row++) {
    memcpy(Array2d_ptr(*dest, dest_row + row, dest_col),
           Array2d_ptr(*src, src_row + row, src_col),
           width);

    if (dest == &out && dest_col + width == dest->n_cols)
      writeNewline(Array2d_ptr(*dest, dest_row + row, dest->n_cols),
                   newline_type);
  }
}

