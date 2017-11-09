/*
  zlines

  A tool for storing a large number of text lines in a compressed file
  and an API for accessing those lines efficiently.

  This file is the command line tool for creating and managing zlines files.

  https://github.com/oshkosher/bioio/tree/master/zlines

  Ed Karrels, ed.karrels@gmail.com, January 2017
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <assert.h>
#include <errno.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/stat.h>

#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif

#include "zstd.h"
#include "zline_api.h"
#include "common.h"

#define DEFAULT_BLOCK_SIZE (4 * 1024 * 1024)
#define CREATE_FILE_UPDATE_FREQUENCY_BYTES (50*1024*1024)

enum ProgramMode {PROG_CREATE, PROG_DETAILS, PROG_VERIFY, PROG_GET,
                  PROG_PRINT};

typedef uint64_t u64;
typedef int64_t i64;


typedef struct {
  i64 start, end, step;
  int has_start, has_end;
} Range;


typedef struct {
  enum ProgramMode mode;
  int block_size;
  const char *input_filename;
  const char *output_filename;

  /* used in "get" mode */
  Range *line_numbers;
  int line_number_count;

  /* used in "details" mode */
  int flag_blocks, flag_lines;
} Options;

int quiet = 0;

int parseArgs(int argc, char **argv, Options *opt);
void printHelp(void);
int processFile(Options *opt, FILE *input_fp);

int createFile(Options *opt);
int fileDetails(Options *opt);
int verifyFile(Options *opt);
int getLines(Options *opt);
int printLines(Options *opt);

/* Return nonzero on error */
int parseRange(Range *r, const char *s);


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

  case PROG_PRINT:
    return printLines(&opt);
  }  

  return 0;
}


int parseArgs(int argc, char **argv, Options *opt) {
  int argno = 1, i;

  opt->mode = PROG_CREATE;
  opt->block_size = DEFAULT_BLOCK_SIZE;
  quiet = 0;
  opt->input_filename = opt->output_filename = NULL;
  opt->line_numbers = 0;
  opt->line_number_count = 0;
  opt->flag_blocks = opt->flag_lines = 0;

  if (argc < 2) printHelp();
  
  if (!strcmp(argv[argno], "create")) {
    opt->mode = PROG_CREATE;
  } else if (!strcmp(argv[argno], "details")) {
    opt->mode = PROG_DETAILS;
  } else if (!strcmp(argv[argno], "verify")) {
    opt->mode = PROG_VERIFY;
  } else if (!strcmp(argv[argno], "get")) {
    opt->mode = PROG_GET;
  } else if (!strcmp(argv[argno], "print")) {
    opt->mode = PROG_PRINT;
  } else if (!strcmp(argv[argno], "-h")) {
    printHelp();
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
      if (opt->mode == PROG_DETAILS) {
        opt->flag_blocks = 1;
      } else {
        argno++;
        if (argno >= argc) printHelp();
        if (1 != sscanf(argv[argno], "%d", &opt->block_size) ||
            opt->block_size <= 1) {
          fprintf(stderr, "Invalid block size: \"%s\"\n", argv[argno]);
          return 1;
        }
      }
    }
      
    else if (!strcmp(argv[argno], "-q")) {
      quiet = 1;
    }
      
    else if (!strcmp(argv[argno], "-l")) {
      opt->flag_lines = 1;
    }
      
    else {
      fprintf(stderr, "Unrecognized option: \"%s\"\n", argv[argno]);
      return 1;
    }

    argno++;
  }

  switch (opt->mode) {
  case PROG_CREATE:
    if (argno+2 != argc) printHelp();
    opt->output_filename = argv[argno++];
    opt->input_filename = argv[argno++];
    break;

  case PROG_DETAILS:
    if (argno+1 != argc) printHelp();
    opt->input_filename = argv[argno++];
    break;

  case PROG_VERIFY:
    if (argno+2 != argc) printHelp();
    opt->output_filename = argv[argno++];
    opt->input_filename = argv[argno++];
    break;

  case PROG_PRINT:
    if (argno+1 != argc) printHelp();
    opt->input_filename = argv[argno++];
    break;

  case PROG_GET:
    if (argno+1 >= argc) printHelp();
    opt->input_filename = argv[argno++];

    opt->line_number_count = argc - argno;
    opt->line_numbers = (Range*) malloc(sizeof(Range) * opt->line_number_count);
    assert(opt->line_numbers);

    for (i=0; argno < argc; argno++,i++) {
      if (parseRange(&opt->line_numbers[i], argv[argno])) {
        fprintf(stderr, "Invalid line number range \"%s\"\n", argv[argno]);
        return 1;
      }
    }
  }
  
  return 0;
}


void printHelp(void) {
  fprintf(stderr,
          "\n"
          "  zlines create [options] <output zlines file> <input text file>\n"
          "    if input text file is \"-\", use stdin\n"
          "    options:\n"
          "      -b <block size> : size (in bytes) of compression blocks\n"
          "      -q : don't print status output\n"
          "\n"
          "  zlines print <zlines file>\n"
          "    prints every line in the file\n"
          "\n"
          "  zlines details [options] <zlines file>\n"
          "    prints internal details about the data encoded in the file\n"
          "    options:\n"
          "      -b: print details about each compressed block\n"
          "      -l: print details about each line of data\n"
          "\n"
          "  zlines verify <zlines file> <text file>\n"
          "    tests if the zlines file matches the given text file\n"
          "\n"
          "  zlines get <zlines file> <line#> [<line#> ...]\n"
          "    extracts the given lines from the file and prints them\n\n"
          "    line#: index of the line, starting from 0\n"
          "    Negative numbers count back from the end: -1 is the last line\n"
          "    Ranges in the style of Python array slices are also supported.\n"
          "    For example:  0:5 -10: :100 0:100:3 ::-1\n"
          "\n");
  exit(1);
}


const char *skipWhitespace(const char *p) {
  while (isspace(*p)) p++;
  return p;
}


int isNumber(char c) {
  return isdigit(c) || c=='-' || c=='+';
}


/* Parse number ranges like Python's array slicing:
   23       just line 23
   10:100   lines [10..100)
   10:      from line 10 to the end
   :100     first 100 lines
   0-20:2   line [0..20), skipping by 2: 0, 2, 4, ... 18
   -1       last line
   -10:     last 10 lines
   5:-3     all but the first 5 and last 3 lines
   :        full range (start=0, has_end=0, step=1)
   ::2      full range, every other element

   Return nonzero on error. */
int parseRange(Range *r, const char *s) {
  int consumed;
  const char *p = skipWhitespace(s);

  r->start = r->end = 0;
  r->has_start = r->has_end = 0;
  r->step = 1;

  /* parse start */
  if (isNumber(*p)) {
    r->has_start = 1;
    if (1 != sscanf(p, "%" SCNi64 "%n", &r->start, &consumed))
      return 1;
    p += consumed;
    p = skipWhitespace(p);
  }

  /* expecting end of string or the colon after r->start */
  if (*p == 0) {
    if (r->has_start) {
      /* just a number, like "12" */
      /* special case: with a single value, set step to 0 */
      r->step = 0;
      return 0;
    } else {
      return 1;
    }
  } else if (*p == ':') {
    p++;
    p = skipWhitespace(p);
  } else {
    return 1;
  }

  /* expecting end of string, the colon after r->end, or a number for r->end */
  if (isNumber(*p)) {
    if (1 != sscanf(p, "%" SCNi64 "%n", &r->end, &consumed))
      return 1;
    p += consumed;
    p = skipWhitespace(p);
    r->has_end = 1;
  }

  /* expecting end of string or the colon after r->end */
  if (*p == 0) {
    return 0;
  } else if (*p == ':') {
    p++;
    p = skipWhitespace(p);
  }

  /* expecting end of string or r->step */

  if (isNumber(*p)) {
    if (1 != sscanf(p, "%" SCNi64 "%n", &r->step, &consumed))
      return 1;
    p += consumed;
    p = skipWhitespace(p);
  }

  /* expecting end of string */
  if (*p == 0) {
    return 0;
  } else {
    return 1;
  }
}


/* Trim newlines from the end of the line, either Unix-style or DOS.
   Return the new length of the line. */
size_t trimNewline(char *line, size_t len) {
  if (line[len-1] == '\n') {
    line[--len] = 0;
    if (line[len-1] == '\r') {
      line[--len] = 0;
    }
  }
  return len;
}


/* Write one line of status output, leading with a carriage return
   so it overwrites the current output line. */
void statusOutput(u64 line_count, u64 byte_count, u64 file_size) {
  char linebuf[50], bytebuf[50];
  if (quiet) return;
  printf("\r%s lines, %s bytes", commafy(linebuf, line_count),
         commafy(bytebuf, byte_count));
  if (file_size)
    printf(" of %s (%.1f%%)", commafy(bytebuf, file_size),
           byte_count * 100.0 / file_size);
  fflush(stdout);
}


/* Read a text file and create a zlines file from it. */
int createFile(Options *opt) {
  ZlineFile zf;
  char *line = NULL, buf1[50], buf2[50];
  ssize_t line_len;
  size_t buf_len = 0;
  int err = 0;
  u64 idx, total_bytes = 0, total_zblock_size = 0;
  u64 input_file_size = 0, output_file_size;
  u64 min_line_len = UINT64_MAX, max_line_len = 0;
  u64 next_update = CREATE_FILE_UPDATE_FREQUENCY_BYTES;
  u64 overhead;

  /* open the text file */
  FILE *input_fp = openFileOrStdin(opt->input_filename);
  if (!input_fp) return 1;

  /* if it's not stdin, get the size of the file */
  if (input_fp != stdin)
    input_file_size = getFileSize(opt->input_filename);

  /* open the zlines file */
  zf = ZlineFile_create2(opt->output_filename, opt->block_size);
  if (!zf) {
    fprintf(stderr, "Error: cannot write \"%s\"\n", opt->output_filename);
    return 1;
  }

  while (1) {
    /* read a line */
    line_len = getline(&line, &buf_len, input_fp);
    if (line_len == -1) break;
    total_bytes += line_len;

    /* output a status update now and then */
    if (total_bytes >= next_update) {
      statusOutput(ZlineFile_line_count(zf), total_bytes, input_file_size);
      next_update = total_bytes + CREATE_FILE_UPDATE_FREQUENCY_BYTES;
    }

    /* remove the trailing newline from the line */
    line_len = trimNewline(line, line_len);

    /* track the maximum and minimum line lengths */
    if ((u64)line_len > max_line_len) max_line_len = line_len;
    if ((u64)line_len < min_line_len) min_line_len = line_len;

    /* add the line to the zlines file */
    if (ZlineFile_add_line2(zf, line, line_len)) {
      err = 1;
      break;
    }
  }

  /* print a final status update */
  statusOutput(ZlineFile_line_count(zf), total_bytes, input_file_size);

  /* close the zlines file because the index and header aren't written until
     the file is closed. */
  ZlineFile_close(zf);
  free(line);
  if (input_fp != stdin) fclose(input_fp);

  output_file_size = getFileSize(opt->output_filename);

  /* reopen the zlines file for reading */
  zf = ZlineFile_read(opt->output_filename);
  assert(zf);

  /* compute the compressed size of the data */
  for (idx = 0; idx < ZlineFile_get_block_count(zf); idx++)
    total_zblock_size += ZlineFile_get_block_size_compressed(zf, idx);

  overhead = output_file_size - total_zblock_size;
  
  if (!quiet) {
    printf("\nline lengths %" PRIu64 "..%" PRIu64 "\n"
           "compressed to %s bytes in %" PRIu64 " block%s\n"
           "%s bytes overhead, %.2f bytes per line\n", 
           min_line_len, max_line_len,
           commafy(buf1, total_zblock_size),
           ZlineFile_get_block_count(zf), ZlineFile_get_block_count(zf)==1 ? "" : "s",
           commafy(buf2, overhead), (double)overhead / ZlineFile_line_count(zf));
  }
  
  ZlineFile_close(zf);

  return err;
}


int fileDetails(Options *opt) {
  ZlineFile zf;
  u64 i;

  zf = ZlineFile_read(opt->input_filename);
  if (!zf) {
    fprintf(stderr, "Failed to open \"%s\" for reading.\n", opt->input_filename);
    return 1;
  }

  printf("%" PRIu64 " lines, longest line %" PRIu64 " bytes\n",
         ZlineFile_line_count(zf), ZlineFile_max_line_length(zf));
  
  printf("data begins at offset %" PRIu64 "\n", ZlineFile_get_block_offset(zf, 0));
  printf("block index at offset %" PRIu64 "\n", ZlineFile_get_block_index_offset(zf));

  printf("%" PRIu64 " compressed blocks\n", ZlineFile_get_block_count(zf));

  if (opt->flag_blocks) {
    for (i=0; i < ZlineFile_get_block_count(zf); i++) {
      printf("block %" PRIu64 ": %" PRIu64 " lines, %" PRIu64 " bytes->"
             "%" PRIu64 " bytes, offset %" PRIu64 "\n",
             i,
             ZlineFile_get_block_line_count(zf, i),
             ZlineFile_get_block_size_original(zf, i),
             ZlineFile_get_block_size_compressed(zf, i),
             ZlineFile_get_block_offset(zf, i));
    }
  }

  if (opt->flag_lines) {
    for (i=0; i < ZlineFile_line_count(zf); i++) {
      uint64_t block_idx, length, offset;
      ZlineFile_get_line_details(zf, i, &length, &offset, &block_idx);
      
      printf("line %" PRIu64 ": in block %" PRIu64 ", offset %" PRIu64
             ", len %" PRIu64 "\n",
             i, block_idx, offset, length);
    }
  }

  ZlineFile_close(zf);
  
  return 0;
}


int verifyFile(Options *opt) {
  ZlineFile zf;
  u64 line_idx = 0, line_count, buf_len;
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
  buf_len = ZlineFile_max_line_length(zf) + 1;
  extracted_line = (char*) malloc(buf_len);

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
    
    ZlineFile_get_line2(zf, line_idx, extracted_line, buf_len, 0);
    if (strcmp(extracted_line, line)) {
      printf("Line %" PRIu64 " mismatch.\n", line_idx);
      err_count++;
      if (err_count == 10) {
        printf("Too many errors. Exiting.\n");
        return 1;
      }
    }

    line_idx++;
  }

  if (line_idx != line_count) {
    printf("Error: %" PRIu64 " lines in %s, but %s contains %" PRIu64 "\n",
           line_idx, text_filename, zlines_filename, line_count);
    err_count++;
  }

  free(extracted_line);
  free(line);
  ZlineFile_close(zf);
  if (text_file != stdin) fclose(text_file);

  if (err_count == 0)
    printf("No errors\n");
  
  return 0;
}


int checkLineNumbers(int64_t *line_no, int64_t n_lines) {
  if ((*line_no > 0 && *line_no > n_lines)
      || (*line_no < 0 && -*line_no > n_lines)) {
    fprintf(stderr, "Invalid line number: %" PRIi64 "\n", *line_no);
    return 1;
  }
  
  if (*line_no < 0) *line_no += n_lines;
  return 0;
}


void printLine(ZlineFile zf, i64 line_no, char **buf, size_t *buf_size) {
  i64 line_len = ZlineFile_line_length(zf, line_no);
  if (line_len < 0) {
    fprintf(stderr, "Invalid line number: %" PRIi64 "\n", line_no);
    exit(1);
  }

  /* make sure the buffer is big enough */
  if (*buf_size < line_len) {
    *buf_size = MAX(*buf_size * 2, line_len);
    free(*buf);
    *buf = (char*) malloc(*buf_size);
  }

  ZlineFile_get_line2(zf, line_no, *buf, *buf_size, 0);
  puts(*buf);
}



int getLines(Options *opt) {
  ZlineFile zf;
  int i;
  i64 line_idx, file_line_count;
  char *buf;
  size_t buf_len;

  zf = ZlineFile_read(opt->input_filename);
  if (!zf) {
    fprintf(stderr, "Failed to open \"%s\" for reading.\n",
            opt->input_filename);
    return 1;
  }

  buf_len = 100;
  buf = (char*) malloc(buf_len);

  /* number of lines in the file */
  file_line_count = ZlineFile_line_count(zf);

  /* loop through the array of lines requested */
  for (i=0; i < opt->line_number_count; i++) {
    Range r = opt->line_numbers[i];

    assert(r.step != 0 || r.has_start);
    
    if (r.has_start) {
      if (checkLineNumbers(&r.start, file_line_count)) continue;

      /* special case: output a single line */
      if (r.step == 0) {
        r.step = 1;
        r.end = r.start + 1;
        r.has_end = 1;
      }
      
    } else {
      if (r.step > 0)
        r.start = 0;
      else
        r.start = file_line_count - 1;
    }      

    if (r.has_end) {
      if (checkLineNumbers(&r.end, file_line_count)) continue;
    } else {
      if (r.step > 0)
        r.end = file_line_count;
      else
        r.end = -1;
    }

    assert(r.step != 0);
    if (r.step > 0) {
      for (line_idx = r.start; line_idx < r.end; line_idx += r.step) {
        printLine(zf, line_idx, &buf, &buf_len);
      }
    } else {
      for (line_idx = r.start; line_idx > r.end; line_idx += r.step) {
        printLine(zf, line_idx, &buf, &buf_len);
      }
    }
  }

  free(opt->line_numbers);
  free(buf);
  
  ZlineFile_close(zf);
  
  return 0;
}


int printLines(Options *opt) {
  ZlineFile zf;
  u64 i, count, line_len, buf_len;
  char *line;

  zf = ZlineFile_read(opt->input_filename);
  if (!zf) {
    fprintf(stderr, "Failed to open \"%s\" for reading.\n",
            opt->input_filename);
    return 1;
  }

  /* number of lines in the file */
  count = ZlineFile_line_count(zf);

  /* allocate a buffer big enough to hold any line, plus newline and nul */
  buf_len = ZlineFile_max_line_length(zf) + 2;
  line = (char*) malloc(buf_len);
  if (!line) {
    fprintf(stderr, "Out of memory\n");
    return 1;
  }

  /* extract each line and print it */
  for (i = 0; i < count; i++) {
    line_len = ZlineFile_line_length(zf, i);
    ZlineFile_get_line2(zf, i, line, buf_len, 0);
    line[line_len] = '\n';
    fwrite(line, line_len+1, 1, stdout);
  }
  
  ZlineFile_close(zf);
  free(line);
  
  return 0;
}

      
