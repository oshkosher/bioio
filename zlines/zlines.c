#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <assert.h>
#include <inttypes.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "zstd.h"
#include "zline_api.h"

#define DEFAULT_BLOCK_SIZE (4 * 1024 * 1024)

enum ProgramMode {PROG_CREATE, PROG_DETAILS, PROG_VERIFY, PROG_GET};

typedef uint64_t u64;


typedef struct {
  enum ProgramMode mode;
  int block_size;
  const char *input_filename;
  const char *output_filename;

  /* used in "get" mode */
  u64 *line_numbers;
  int line_number_count;
} Options;


int parseArgs(int argc, char **argv, Options *opt);
void printHelp();
int processFile(Options *opt, FILE *input_fp);
FILE *openFileOrStdin(const char *filename);
u64 getFileSize(const char *filename);

int createFile(Options *opt);
int fileDetails(Options *opt);
int verifyFile(Options *opt);
int getLines(Options *opt);

int main(int argc, char **argv) {
  Options opt;
  
  if (parseArgs(argc, argv, &opt)) return 1;

  switch (opt.mode) {
  case PROG_CREATE:
    return createFile(&opt);

  case PROG_DETAILS:
    return fileDetails(&opt);

  case PROG_VERIFY:
    return verifyFile(&opt);

  case PROG_GET:
    return getLines(&opt);
  }  

  return 0;
}


int parseArgs(int argc, char **argv, Options *opt) {
  int argno = 1, i;

  opt->mode = PROG_CREATE;
  opt->block_size = DEFAULT_BLOCK_SIZE;
  opt->input_filename = opt->output_filename = NULL;
  opt->line_numbers = 0;
  opt->line_number_count = 0;

  if (argc < 2) printHelp();
  
  if (!strcmp(argv[argno], "create")) {
    opt->mode = PROG_CREATE;
  } else if (!strcmp(argv[argno], "details")) {
    opt->mode = PROG_DETAILS;
  } else if (!strcmp(argv[argno], "verify")) {
    opt->mode = PROG_VERIFY;
  } else if (!strcmp(argv[argno], "get")) {
    opt->mode = PROG_GET;
  } else {
    fprintf(stderr, "Invalid command: \"%s\"\n", argv[argno]);
    return 1;
  }

  argno++;
  
  while (argno < argc && argv[argno][0] == '-') {
    if (!strcmp(argv[argno], "-")) {
      break;
    }
      
    else if (!strcmp(argv[argno], "-b")) {
      argno++;
      if (argno >= argc) printHelp();
      if (1 != sscanf(argv[argno], "%d", &opt->block_size) ||
          opt->block_size <= 1) {
        fprintf(stderr, "Invalid block size: \"%s\"\n", argv[argno]);
        return 1;
      }
    }

    argno++;
  }

  switch (opt->mode) {
  case PROG_CREATE:
    if (argno+2 != argc) printHelp();
    opt->input_filename = argv[argno++];
    opt->output_filename = argv[argno++];
    break;

  case PROG_DETAILS:
    if (argno+1 != argc) printHelp();
    opt->input_filename = argv[argno++];
    break;

  case PROG_VERIFY:
    if (argno+2 != argc) printHelp();
    opt->input_filename = argv[argno++];
    opt->output_filename = argv[argno++];
    break;

  case PROG_GET:
    if (argno+1 >= argc) printHelp();
    opt->input_filename = argv[argno++];

    opt->line_number_count = argc - argno;
    opt->line_numbers = (u64*) malloc(sizeof(u64) * opt->line_number_count);
    assert(opt->line_numbers);

    i = 0;
    while(argno < argc) {
      if (1 != sscanf(argv[argno], "%" SCNu64, &opt->line_numbers[i])) {
        fprintf(stderr, "Invalid line number \"%s\"\n", argv[argno]);
        return 1;
      }
      argno++;
      i++;
    }
  }
  
  return 0;
}


void printHelp() {
  fprintf(stderr,
          "\n"
          "  zlines create [options] <input text file> <output zlines file>\n"
          "    if input text file is \"-\", use stdin\n"
          "    options:\n"
          "      -b <block size> : size (in bytes) of compression blocks\n"
          "\n"
          "  zlines details <zlines file>\n"
          "    prints internal details about the data encoded in the file\n"
          "\n"
          "  zlines verify <input text file> <zlines file>\n"
          "    tests if the zlines file matches the given text file\n"
          "\n"
          "  zlines get <zlines file> [line#...]\n"
          "    extracts the given lines from the file and prints them\n"
          "\n");
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


u64 getFileSize(const char *filename) {
   struct stat stats;
   if (stat(filename, &stats)) {
    fprintf(stderr, "Error getting size of \"%s\": %s\n",
	    filename, strerror(errno));
    return 0;
  } else {
    return stats.st_size;
  }
}


/* Trim newlines from the end of the line, either Unix-style or DOS. */
size_t trimNewline(char *line, size_t len) {
  while (len > 0 &&
         (line[len-1] == 0x0A ||
          line[len-1] == 0x0D)) {
    line[--len] = 0;
  }
  return len;
}
    

  
int createFile(Options *opt) {
  ZlineFile *zf;
  char *line = NULL;
  ssize_t line_len;
  size_t buf_len = 0;
  int err = 0;
  u64 idx, total_line_bytes = 0, total_zblock_size = 0, file_size;
  u64 line_count, block_count, min_line_len = UINT64_MAX, max_line_len = 0;

  FILE *input_fp = openFileOrStdin(opt->input_filename);
  if (!input_fp) return 1;
  
  zf = ZlineFile_create(opt->output_filename, opt->block_size);
  if (!zf) {
    fprintf(stderr, "Error: cannot write \"%s\"\n", opt->output_filename);
    return 1;
  }

  while (1) {
    line_len = getline(&line, &buf_len, input_fp);
    if (line_len == -1) break;
    line_len = trimNewline(line, line_len);
    total_line_bytes += line_len;

    if ((u64)line_len > max_line_len) max_line_len = line_len;
    if ((u64)line_len < min_line_len) min_line_len = line_len;
    
    if (ZlineFile_add_line(zf, line, line_len)) {
      err = 1;
      break;
    }
  }
  
  free(line);
  ZlineFile_close(zf);
  if (input_fp != stdin) fclose(input_fp);

  file_size = getFileSize(opt->output_filename);

  zf = ZlineFile_read(opt->output_filename);
  assert(zf);

  for (idx = 0; idx < zf->block_count; idx++)
    total_zblock_size += zf->blocks[idx].compressed_length;

  line_count = zf->line_count;
  block_count = zf->block_count;

  printf("%" PRIu64 " lines of length %" PRIu64 "..%" PRIu64 ", %"
         PRIu64 " bytes\n"
         "compressed to %" PRIu64 " blocks, %"
         PRIu64 " bytes with %" PRIu64 " bytes overhead\n", 
         line_count, min_line_len, max_line_len,
         total_line_bytes, block_count,
         total_zblock_size, file_size - total_zblock_size);
         
  ZlineFile_close(zf);

  return err;
}


int fileDetails(Options *opt) {
  ZlineFile *zf;
  u64 i;

  zf = ZlineFile_read(opt->input_filename);
  if (!zf) {
    fprintf(stderr, "Failed to open \"%s\" for reading.\n", opt->input_filename);
    return 1;
  }

  printf("data starts at offset %" PRIu64 "\n", zf->data_offset);
  printf("index starts at offset %" PRIu64 "\n", zf->index_offset);
  
  printf("%" PRIu64 " compressed blocks\n", zf->block_count);
  for (i=0; i < zf->block_count; i++) {
    printf("block %" PRIu64 ": offset %" PRIu64 ", compressed len %" PRIu64
           ", decompressed len %" PRIu64 "\n",
           i, zf->blocks[i].offset, zf->blocks[i].compressed_length,
           zf->blocks[i].decompressed_length);
  }

  printf("%" PRIu64 " lines\n", zf->line_count);
  for (i=0; i < zf->line_count; i++) {
    printf("line %" PRIu64 ": in block %" PRIu64 ", offset %" PRIu64 ", len %" PRIu64
           "\n",
           i, zf->lines[i].block_idx, zf->lines[i].offset, zf->lines[i].length);
  }    

  ZlineFile_close(zf);
  
  return 0;
}


int verifyFile(Options *opt) {
  ZlineFile *zf;
  u64 line_idx = 0, line_count;
  char *line = NULL, *extracted_line;
  const char *text_filename, *zlines_filename;
  ssize_t line_len;
  size_t line_cap = 0;
  FILE *text_file;
  int err_count = 0;

  zlines_filename = opt->output_filename;
  text_filename = opt->input_filename;
  
  zf = ZlineFile_read(zlines_filename);
  if (!zf) {
    fprintf(stderr, "Failed to open \"%s\" for reading.\n", zlines_filename);
    return 1;
  }

  line_count = ZlineFile_line_count(zf);

  text_file = openFileOrStdin(text_filename);
  if (!text_file) return 1;

  while (1) {
    line_len = getline(&line, &line_cap, text_file);
    if (line_len == -1) break;
    line_len = trimNewline(line, line_len);

    if (line_idx >= line_count) {
      printf("Error: %" PRIu64 " lines in %s, but %s contains more\n",
             line_count, zlines_filename, text_filename);
      return 1;
    }
    
    extracted_line = ZlineFile_get_line(zf, line_idx, NULL);
    if (strcmp(extracted_line, line)) {
      printf("Line %" PRIu64 " mismatch.\n  original: %s\n extracted: %s\n",
             line_idx, line, extracted_line);
      err_count++;
      if (err_count == 10) {
        printf("Too many errors. Exiting.\n");
        return 1;
      }
    }
    free(extracted_line);

    line_idx++;
  }

  if (line_idx != line_count) {
    printf("Error: %" PRIu64 " lines in %s, but %s contains %" PRIu64 "\n",
           line_idx, text_filename, zlines_filename, line_count);
    err_count++;
  }

  free(line);
  ZlineFile_close(zf);
  if (text_file != stdin) fclose(text_file);

  if (err_count == 0)
    printf("No errors\n");
  
  return 0;
}


int getLines(Options *opt) {
  ZlineFile *zf;
  int i;
  u64 line_idx, file_line_count;
  char *line;

  zf = ZlineFile_read(opt->input_filename);
  if (!zf) {
    fprintf(stderr, "Failed to open \"%s\" for reading.\n",
            opt->input_filename);
    return 1;
  }

  /* number of lines in the file */
  file_line_count = ZlineFile_line_count(zf);

  /* loop through the array of lines requested */
  for (i=0; i < opt->line_number_count; i++) {

    line_idx = opt->line_numbers[i];

    /* check that the line index is in bounds */
    if (line_idx >= file_line_count) {
      printf("invalid line number: %" PRIu64 " (max %" PRIu64 ")\n",
             line_idx, file_line_count - 1);
    } else {

      /* extract one line from the file */
      line = ZlineFile_get_line(zf, line_idx, NULL);

      puts(line);
      free(line);
    }
  }

  free(opt->line_numbers);
  
  ZlineFile_close(zf);
  
  return 0;
}

      
