#include <string.h>
#include <sys/mman.h>
#include "common.h"

typedef uint64_t u64;

int main(int argc, char **argv) {
  File2d file1, file2;
  Array2d array1, array2;
  u64 file1_len, file2_len;
  int row;
  double start_time, elapsed;

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
  file2_len = array2.row_stride * array2.n_rows;

  if (mapFile(argv[2], 1, &array2.data, &file2_len)) {
    printf("Failed to open %s\n", argv[2]);
    return 1;
  }

  /* start by writing all the newlines */
  for (row=0; row < array2.n_rows; row++) {
    char *p = Array2d_ptr(&array2, row, array2.n_cols);
    writeNewline(p, file1.newline_type);
  }

  start_time = getSeconds();
  transpose(&array2, 0, 0,
            &array1, 0, 0,
            file1.n_rows, file1.n_cols);

  elapsed = getSeconds() - start_time;
  printf("%.3f seconds, %.3f MiB/s\n",
         elapsed, file1_len / (1024 * 1024 * elapsed));

  if (munmap(array1.data, file1_len))
    printf("Failed to close %s\n", file1.filename);
  if (munmap(array2.data, file2_len))
    printf("Failed to close %s\n", file2.filename);
  
  return 0;
}

