#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <time.h>

const char *charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789+-";

int main(int argc, char **argv) {
  int rows, cols, r, c;
  char *row;
  
  if (argc != 3) {
    printf("\n  create_2d_data <rows> <cols>\n\n"
           "  Create a bunch of random data for testing transpose.\n\n");
    return 1;
  }

  rows = atoi(argv[1]);
  cols = atoi(argv[2]);

  if (rows <= 0 || cols <= 0) {
    printf("Invalid size\n");
    return 1;
  }

  row = (char*) malloc(cols + 2);
  assert(row);

  row[cols] = '\n';
  row[cols+1] = 0;
  
  srand((unsigned)time(NULL));
  
  for (r=0; r < rows; r++) {
    for (c=0; c < cols; c++) {
      /* putchar('a' + rand() % 26); */
      /* putchar(charset[rand() & 63]); */
      row[c] = charset[rand() & 63];
    }
    /* putchar('\n'); */
    fwrite(row, 1, cols+1, stdout);
  }

  return 0;
}
