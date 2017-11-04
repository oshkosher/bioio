/*
  Performance test - scan through every line of text in a zlines file.

  https://github.com/oshkosher/bioio/tree/master/zlines

  Ed Karrels, ed.karrels@gmail.com, January 2017
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include "zline_api.h"


int main(int argc, char **argv) {
  ZlineFile *zf;
  int64_t i, count, bytes = 0, buf_len;
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
  buf_len = ZlineFile_max_line_length(zf) + 1;
  line = (char*) malloc(buf_len);

  /* extract each line (but don't do anything with it) */
  for (i = 0; i < count; i++) {
    ZlineFile_get_line2(zf, i, line, buf_len, 0);
    bytes += strlen(line);
  }

  free(line);
  ZlineFile_close(zf);

  printf("%ld lines read, %ld bytes.\n", count, bytes);

  return 0;
}

