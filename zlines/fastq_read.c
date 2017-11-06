#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>
#include <string.h>
#include "zline_api.h"

#define MIN(a,b) ((a)<(b)?(a):(b))
#define MAX(a,b) ((a)>(b)?(a):(b))


static void printHelp() {
  fprintf(stderr, "\n"
          "  fastq_read <zlines-file> <first-read> <read-count> <which-lines>\n"
          "    Extract \"reads\" (blocks of 4 text lines) from the given zlines file,\n"
          "    printing them to stdout.\n"
          "\n"
          "    zlines-file: the data file, in 'zlines' format\n"
          "    first-read: index of the first read, counting from 0\n"
          "    read-count: number of reads to extract\n"
          "    which-lines: which of the 4 lines of each read to print, counting from 1\n"
          "      For example, \"1\" extracts the first  line of each read.\n"
          "      \"24\" extracts the second and fourth lines of each read.\n"
          "\n");
  exit(1);
}


int main(int argc, const char **argv) {
  int i, n_selected, selected[4];
  const char *filename, *which_lines;
  int64_t first_read, read_count, total_read_count, read_no;
  ZlineFile *zf;

  if (argc != 5) printHelp();
  filename = argv[1];
  zf = ZlineFile_read(filename);
  if (!zf) {
    fprintf(stderr, "Cannot open \"%s\"\n", filename);
    return 1;
  }

  total_read_count = ZlineFile_line_count(zf) / 4;
  
  if (1 != sscanf(argv[2], "%" SCNi64, &first_read) ||
      first_read < 0 || first_read >= total_read_count) {
    fprintf(stderr, "Invalid first read: %s\n", argv[2]);
    return 1;
  }

  if (1 != sscanf(argv[3], "%" SCNi64, &read_count) ||
      read_count < 0) {
    fprintf(stderr, "Invalid read count: %s\n", argv[3]);
    return 1;
  }

  which_lines = argv[4];
  if (strlen(which_lines) > 4) {
    fprintf(stderr, "Error: which-lines can be 4 lines at most\n");
    return 1;
  }
  n_selected = strlen(which_lines);
  /* print nothing! */
  if (n_selected == 0) return 0;

  for (i=0; i < n_selected; i++) {
    if (which_lines[i] < '1' || which_lines[i] > '4') {
      fprintf(stderr, "Invalid which-lines: \"%s\"\n", which_lines);
      return 1;
    }
    selected[i] = which_lines[i] - '1';
  }

  for (read_no = first_read;
       read_no < first_read + read_count && read_no < total_read_count;
       read_no++) {

    for (i=0; i < n_selected; i++) {
      char *line = ZlineFile_get_line(zf, read_no * 4 + selected[i]);
      puts(line);
      free(line);
    }
  }

  ZlineFile_close(zf);
  return 0;
}

      
    
  
