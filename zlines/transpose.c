/*
  Do a bytewise tranpose of a text file.

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

#define NEWLINE_UNIX 1
#define NEWLINE_DOS 2

typedef struct {
  /* If nonzero, blocks read will progress to the right, then down, and
     blocks written will progress down, then right.
     If zero, the the opposite. */
  int move_right;

  /* The data is processed in blocks. This is the number of rows in 
     each block read and number of columns in each block written. */
  int read_block_height;

  /* Number of columns in each block read and number of rows
     in each block written. */
  int read_block_width;

  int cache_ob_size;

  int newline_type;

  const char *in_file;
  char *out_file;
  int n_rows, n_cols;  /* shape of input file */
  int in_file_stride, out_file_stride;  /* includes newline */
} Options;

#define MIN(a,b) ((a)<(b)?(a):(b))
#define MAX(a,b) ((a)>(b)?(a):(b))

void printHelp();
int getFileDimensions(const char *in_file, u64 length,
                      int *n_cols, int *n_rows, int *newline_type);
#define newlineLength(newline_type) (newline_type)
#define newlineName(newline_type) ((newline_type)==(NEWLINE_DOS)?"DOS":"unix")
void writeNewline(char *dest, int newline_type);

void transposeBlocks(Options *opt);
void transposeCacheOblivious(Options *opt);
void transposeCacheObliviousRecurse(Options *opt,
                                    int block_top, int block_left,
                                    int block_height, int block_width);
void transposeTile(Options *opt, int block_left, int block_right,
                   int block_top, int block_bottom);



int main(int argc, char **argv) {
  int result = 0, newline_type;
  int n_rows, n_cols, newline_len;
  u64 in_file_len, out_file_len = 0, byte_count;
  char *out_file;
  const char *in_file;
  Options opt;
  double start_time, elapsed, mbps;

  if (argc != 3) printHelp();

  opt.move_right = 0;
  opt.read_block_height = 256;  /* no more than 512 */
  opt.read_block_width = 256;   /* at least 256 */
  opt.cache_ob_size = 256;
  
  start_time = getSeconds();
  if (mapFile(argv[1], 0, (char**)&in_file, &in_file_len)) {
    fprintf(stderr, "Failed to open %s\n", argv[1]);
    return 1;
  }
  /* printf("map %s in %.3fs\n", argv[1], getSeconds() - start_time); */

  start_time = getSeconds();
  if (getFileDimensions(in_file, in_file_len, &n_cols, &n_rows, &newline_type))
    goto fail;
  /* printf("get size of %s in %.3fs\n", argv[1], getSeconds() - start_time); */

  printf("%s has %d rows of length %d with %s line endings\n",
         argv[1], n_rows, n_cols, newlineName(newline_type));

  newline_len = newlineLength(newline_type);
  out_file_len = (u64)n_cols * (n_rows + newline_len);

  start_time = getSeconds();
  if (mapFile(argv[2], 1, &out_file, &out_file_len)) {
    fprintf(stderr, "Failed to open %s\n", argv[2]);
    return 1;
  }
  /* printf("map %s in %.3fs\n", argv[2], getSeconds() - start_time); */

  opt.newline_type = newline_type;

  start_time = getSeconds();
  opt.in_file = in_file;
  opt.out_file = out_file;
  opt.n_rows = n_rows;
  opt.n_cols = n_cols;
  opt.in_file_stride = n_cols + newline_len;
  opt.out_file_stride = n_rows + newline_len;
  
  /* transposeBlocks(&opt); */
  transposeCacheOblivious(&opt);

  putchar('\n');
  
  byte_count = (u64)n_rows*n_cols;
  elapsed = getSeconds() - start_time;
  mbps = byte_count / (elapsed * 1024 * 1024);
  printf("transpose %dx%d = %" PRIu64" bytes in %.3fs at %.1f MiB/s\n",
         n_rows, n_cols, byte_count, elapsed, mbps);


 fail:
  if (in_file) munmap((char*)in_file, in_file_len);
  if (out_file) munmap(out_file, out_file_len);
  
  return result;
}


void printHelp() {
  printf("\n  transpose <input_file> <output_file>\n"
         "  Do a bytewise transpose of the lines of the given file.\n"
         "  Every line in the file must be the same length.\n\n");
  exit(1);
}


int getFileDimensions(const char *in_file, u64 length,
                      int *n_cols, int *n_rows,
                      int *newline_type) {
  const char *first_newline;
  int line_length;
  u64 row;

  first_newline = strchr(in_file, '\n');
  if (!first_newline) {
    fprintf(stderr, "Invalid input file: no line endings found\n");
    return -1;
  }

  if (first_newline == in_file) {
    fprintf(stderr, "Invalid input file: first line is empty\n");
    return -1;
  }

  if (first_newline[-1] == '\r') {
    *newline_type = NEWLINE_DOS;
  } else {
    *newline_type = NEWLINE_UNIX;
  }
  line_length = (first_newline - in_file) + 1;
  *n_cols = line_length - newlineLength(*newline_type);

  *n_rows = length / line_length;
  if (length != *n_rows * (u64)line_length) {
    fprintf(stderr, "Invalid input file: uneven line lengths "
            "(rows appear to be %d bytes each, but that doesn't evenly "
            "divide the file length, %" PRIu64 "\n", line_length, length);
    return -1;
  }

  /* check a few more rows */

  for (row=10; row < *n_rows; row *= 10) {
    /* printf("check row %d\n", (int)row); */
    if (in_file[row * line_length - 1] != '\n') {
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


void transposeTile(Options *opt, int block_left, int block_right,
                   int block_top, int block_bottom) {
  int x, y;
  static u64 bytes_done = 0, next_report = 100*1000*1000;

  /*
  printf("transpose (%d,%d) - (%d,%d)\n", block_top, block_left,
         block_bottom, block_right);
  */
  
  for (x = block_left; x < block_right; x++) {
    for (y = block_top; y < block_bottom; y++) {
      
      opt->out_file[(u64)x * opt->out_file_stride + y] =
        opt->in_file[(u64)y * opt->in_file_stride + x];

      /* printf("in(%d,%d) -> out(%d,%d)\n", y, x, x, y); */

    }

    if (block_bottom == opt->n_rows) {
      writeNewline(opt->out_file + (u64)x * opt->out_file_stride
                   + opt->n_rows,
                   opt->newline_type);
    }
  }

  bytes_done += (u64) (block_right - block_left) * (block_bottom - block_top);
  if (bytes_done > next_report) {
    printf("\r%" PRIu64 " of %" PRIu64 " bytes done",
           bytes_done, (u64)opt->n_rows * opt->n_cols);
    fflush(stdout);
    next_report += 100*1000*1000;
  }
}  


void transposeBlocks(Options *opt) {

  int block_left, block_top, block_right, block_bottom;

  for (block_left = 0; block_left < opt->n_cols;
       block_left += opt->read_block_width) {

    block_right = MIN(block_left + opt->read_block_width, opt->n_cols);

    for (block_top = 0; block_top < opt->n_rows;
         block_top += opt->read_block_height) {

      block_bottom = MIN(block_top + opt->read_block_height, opt->n_rows);

      transposeTile(opt, block_left, block_right, block_top, block_bottom);

    }

    printf("\r%d rows written", block_right);
    fflush(stdout);

  }
  putchar('\n');
}


void transposeCacheOblivious(Options *opt) {
  transposeCacheObliviousRecurse(opt, 0, 0, opt->n_rows, opt->n_cols);
}

void transposeCacheObliviousRecurse(Options *opt,
                                    int block_top, int block_left,
                                    int block_height, int block_width) {

  if (block_height > opt->cache_ob_size || block_width > opt->cache_ob_size) {
    int half;
    if (block_height > block_width) {
      half = block_height / 2;
      transposeCacheObliviousRecurse(opt, block_top, block_left,
                                     half, block_width);
      transposeCacheObliviousRecurse(opt, block_top+half, block_left,
                                     block_height-half, block_width);
    } else {
      half = block_width / 2;
      transposeCacheObliviousRecurse(opt, block_top, block_left,
                                     block_height, half);
      transposeCacheObliviousRecurse(opt, block_top, block_left+half,
                                     block_height, block_width-half);
    }
    return;
  }      

  transposeTile(opt, block_left, block_left + block_width,
                block_top, block_top + block_height);
}

                
