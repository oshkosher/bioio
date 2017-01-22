/*
  Performance test - scan through every line of text in a zlines file.
*/
  

#include <stdio.h>
#include <stdlib.h>
#include "zline_api.h"


int main(int argc, char **argv) {
  ZlineFile *zf;
  long i, count;
  char *line = NULL;

  if (argc != 2) {
    printf("\n  zlines_test <zlines_file>\n\n");
    return 1;
  }

  /* open the file */
  zf = ZlineFile_read(argv[1]);
  if (!zf) {
    printf("Failed to open %s\n", argv[1]);
    return 1;
  }

  /* get the number of lines in the file */
  count = ZlineFile_line_count(zf);

  /* allocate a buffer big enough to hold any line */
  line = (char*) malloc(ZlineFile_max_line_length(zf) + 1);

  /* extract each line (but don't do anything with it) */
  for (i = 0; i < count; i++) {
    ZlineFile_get_line(zf, i, line);
  }

  free(line);
  ZlineFile_close(zf);

  printf("%ld lines read.\n", count);

  return 0;
}

