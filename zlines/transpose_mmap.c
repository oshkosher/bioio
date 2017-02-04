#include <string.h>
#include <sys/mman.h>
#include "common.h"

typedef uint64_t u64;

#define CACHE_OBLIVIOUS_CUTOFF 128
#define STATUS_OUTPUT_FREQUENCY (30*1024*1024)

int quiet = 0;
u64 bytes_done = 0, total_bytes = 0, next_update = 0;
double start_time;

void statusUpdate(int final);

void myTranspose
(Array2d *dest, int dest_row, int dest_col,
 Array2d *src, int src_row, int src_col,
 int height, int width);


int main(int argc, char **argv) {
  File2d file1, file2;
  Array2d array1, array2;
  u64 file1_len, file2_len;
  int row;
  double elapsed;

  if (argc != 3) {
    printf("\n  transpose_mmap f1 f2\n"
           "  Do a bytewise file transpose using mmap.\n\n");
    return 1;
  }

  /* open file1 and file2 just to get their dimensions */
  if (File2d_open(&file1, argv[1], 0)) {
    printf("Failed to open %s\n", argv[1]);
    return 1;
  }
  File2d_close(&file1);
    
  if (mapFile(argv[1], 0, &array1.data, &file1_len)) {
    printf("Failed to open %s\n", argv[1]);
    return 1;
  }
  
  array1.n_cols = file1.n_cols;
  array1.n_rows = file1.n_rows;
  array1.row_stride = file1.row_stride;

  array2.n_cols = file1.n_rows;
  array2.n_rows = file1.n_cols;
  array2.row_stride = file1.n_rows + newlineLength(file1.newline_type);
  file2_len = (u64)array2.row_stride * array2.n_rows;

  if (mapFile(argv[2], 1, &array2.data, &file2_len)) {
    printf("Failed to open %s\n", argv[2]);
    return 1;
  }

  total_bytes = (u64)array1.n_rows * array1.n_cols;
  
  /* start by writing all the newlines */
  for (row=0; row < array2.n_rows; row++) {
    char *p = Array2d_ptr(&array2, row, array2.n_cols);
    writeNewline(p, file1.newline_type);
  }

  start_time = getSeconds();
  myTranspose(&array2, 0, 0,
              &array1, 0, 0,
              file1.n_rows, file1.n_cols);
  statusUpdate(1);

  elapsed = getSeconds() - start_time;
  printf("%.3f seconds, %.3f MiB/s\n",
         elapsed, file1_len / (1024 * 1024 * elapsed));

  if (munmap(array1.data, file1_len))
    printf("Failed to close %s\n", file1.filename);
  if (munmap(array2.data, file2_len))
    printf("Failed to close %s\n", file2.filename);
  
  return 0;
}


void myTranspose
(Array2d *dest, int dest_row, int dest_col,
 Array2d *src, int src_row, int src_col,
 int height, int width) {

  if (height > CACHE_OBLIVIOUS_CUTOFF ||
      width > CACHE_OBLIVIOUS_CUTOFF) {
    int half;
    if (height > width) {
      half = height / 2;
      myTranspose(dest, dest_row, dest_col,
                  src, src_row, src_col,
                  half, width);
      myTranspose(dest, dest_row, dest_col+half,
                  src, src_row+half, src_col,
                  height-half, width);
    } else {
      half = width / 2;
      myTranspose(dest, dest_row, dest_col,
                  src, src_row, src_col,
                  height, half);
      myTranspose(dest, dest_row+half, dest_col,
                  src, src_row, src_col+half,
                  height, width-half);
    }
    return;
  }      

  transposeTile(dest, dest_row, dest_col,
                src, src_row, src_col, height, width);
  
  bytes_done += (u64)height * width;
  statusUpdate(0);
}


void statusUpdate(int final) {
  char buf1[50], buf2[50];
  double elapsed, rate, remaining;

  if (quiet) return;
  if (!final && bytes_done < next_update) return;

  elapsed = getSeconds() - start_time;
  rate = elapsed==0 ? 0 : bytes_done / elapsed;
  remaining = rate==0 ? 0 : (total_bytes - bytes_done) / rate;
  
  printf("\r%s of %s bytes done, %.1fs elapsed, %.1fs remaining",
         commafy(buf1, bytes_done), commafy(buf2, total_bytes),
         elapsed, remaining);
  
  next_update = bytes_done + STATUS_OUTPUT_FREQUENCY;
  if (final) putchar('\n');
  fflush(stdout);
}
