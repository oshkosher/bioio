#include <string.h>
#include "common.h"

typedef uint64_t u64;

int main(int argc, char **argv) {
  File2d file1, file2;
  Array2d array1, array2;
  u64 file1_len, file2_len;
  char *c1, *c2;
  int row, col;

  if (argc != 3) {
    printf("\n  check_transpose f1 f2\n"
           "  Check that file2 is the bytewise transpose of file1\n\n");
    return 1;
  }

  /* open file1 and file2 just to get their dimensions */
  if (File2d_open(&file1, argv[1], 0)) {
    printf("Failed to open %s\n", argv[1]);
    return 1;
  }
  File2d_close(&file1);

  if (File2d_open(&file2, argv[2], 0)) {
    printf("Failed to open %s\n", argv[2]);
    return 1;
  }
  File2d_close(&file2);
    
  if (mapFile(argv[1], 0, &array1.data, &file1_len)) {
    printf("Failed to open %s\n", argv[1]);
    return 1;
  }

  if (mapFile(argv[2], 0, &array2.data, &file2_len)) {
    printf("Failed to open %s\n", argv[2]);
    return 1;
  }

  array1.n_cols = file1.n_cols;
  array1.n_rows = file1.n_rows;
  array1.row_stride = file1.row_stride;

  array2.n_cols = file2.n_cols;
  array2.n_rows = file2.n_rows;
  array2.row_stride = file2.row_stride;
  
  for (row=0; row < array1.n_rows; row++) {
    for (col=0; col < array1.n_cols; col++) {
      c1 = Array2d_ptr(&array1, row, col);
      c2 = Array2d_ptr(&array2, col, row);
      if (*c1 != *c2) {
        printf("mismatch at input row %d, col %d, '%c' != '%c'\n",
               row+1, col+1, *c1, *c2);
        return 1;
      }
    }
  }

  printf("OK\n");
  return 0;
}

