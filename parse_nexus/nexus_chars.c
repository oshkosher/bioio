/* 
   Extract the "characters" data from a Nexus file.
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <limits.h>
#include "nexus_parse.h"

int rows_read = 0, min_len = INT_MAX, max_len = 0;

int printHelp() {
  printf("\n  nexus_chars <input_file>\n"
         "  Output just the 'characters' data from a Nexus file\n"
         "  Specify \"-\" as the input file to read from stdin.\n\n");
  exit(1);
}


FILE *openFileOrStdin(const char *filename) {
  FILE *f = stdin;
  
  if (strcmp(filename, "-")) {
    f = fopen(filename, "rt");
    if (!f) {
      fprintf(stderr, "Error: cannot read \"%s\"\n", filename);
    }
  }
  return f;
}


void chars_item(void *user_data, const char *name, const char *data) {
  int len = strlen(data);
  puts(data);
  if (len < min_len) min_len = len;
  if (len > max_len) max_len = len;
  rows_read++;
}


void section_end(void *user_data, int section_id, int line_no,
                 long file_offset) {
  if (section_id == NEXUS_SECTION_CHARACTERS) {
    /*
    fprintf(stderr, "%d rows read, lengths %d..%d\n", rows_read,
            min_len, max_len);
    */
    exit(0);
  }
}


int main(int argc, char **argv) {
  int result;
  FILE *inf;
  NexusParseCallbacks callback_functions = {0};

  if (argc != 2) printHelp();

  inf = openFileOrStdin(argv[1]);
  if (!inf) return 1;

  callback_functions.section_end = section_end;
  callback_functions.chars_item = chars_item;

  result = nexus_parse_file(inf, NULL, &callback_functions);

  if (inf != stdin) fclose(inf);
  
  if (result) {
    printf("Errors encountered.\n");
  } else {
    printf("Parse OK.\n");
  }
  
  return 0;
}



