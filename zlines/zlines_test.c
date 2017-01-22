/*
  Performance test - scan through every line of text in a zlines file.
*/
  

#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include "zline_api.h"


int main(int argc, char **argv) {
  ZlineFile *zf;
  uint64_t i, count;

  if (argc != 2) {
    printf("\n  zlines_test <zlines_file>\n\n");
    return 1;
  }

  zf = ZlineFile_read(argv[1]);
  if (!zf) {
    printf("Failed to open %s\n", argv[1]);
    return 1;
  }

  count = ZlineFile_line_count(zf);
  for (i = 0; i < count; i++) {
    char *line = ZlineFile_get_line(zf, i, NULL);
    free(line);
  }

  printf("%" PRIu64 " lines read.\n", count);

  return 0;
}

