/*
  Do a bytewise tranpose of a text file.

  For example, given this input:
    RGk
    ebU
    LwP
    Bkl
    Jd7
  produce this output:
    ReLBJ
    Gbwkd
    kUPl7

  The goal of this is to support arbitrarily large data sets (much larger than
  memory) efficiently.

  Ed Karrels, edk@illinois.edu, January 2017
*/
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include "common.h"

typedef uint64_t u64;

#define DEFAULT_MEMORY_SIZE (32 * 1024 * 1024)
#define VERBOSE 0

FILE *input_file, *output_file;
u64 memory_size;
const char *temp_directory;
int input_width, input_height;  /* total input size */
int read_buffer_rows_filled;
Array2d read_buffer, write_buffer;

char *temp_file_name = NULL;
/* int temp_fd = -1; */
FILE *temp_file = NULL;



void printHelp();
int parseArgs(int argc, char **argv);
size_t trimNewline(char *line, size_t len);
int allocateBuffers();
void deallocateBuffers();

int insureTempFileExists();
u64 tempFileOffset(int row, int col);
u64 tempFileOffsetTx(int row, int col);
int tempFileWrite(int row, int col, const char *src, u64 len);
int tempFileRead(int row, int col, char *dest, u64 len);
int tempFileReadTx(int row, int col, char *dest, u64 len);
int removeTempFile();

int readFile(FILE *input_file);
int addLine(const char *line, int line_no);
int transposeData();
int writeFile(FILE *output_file);

int main(int argc, char **argv) {
  double initial_time, start_time, elapsed, mbps;
  u64 file_size;

  if (parseArgs(argc, argv)) return 1;

  initial_time = start_time = getSeconds();
  if (readFile(input_file)) return 1;
  elapsed = getSeconds() - start_time;
  file_size = (u64)input_height * input_width;
  mbps = file_size / (1024*1024 * elapsed);
  fprintf(stderr, "%dx%d=%" PRIu64 " bytes read in %.3fs, %.3f MiB/s\n",
          input_height, input_width, file_size, elapsed, mbps);

  if (input_file != stdin) fclose(input_file);

  start_time = getSeconds();
  if (transposeData()) return 1;
  fprintf(stderr, "Transposed in %.3fs\n", getSeconds() - start_time);

  start_time = getSeconds();
  if (writeFile(output_file)) return 1;
  if (output_file != stdout) fclose(output_file);
  elapsed = getSeconds() - start_time;
  mbps = file_size / (1024*1024 * elapsed);
  fprintf(stderr, "Result written in %.3fs, %.3f MiB/s\n", elapsed, mbps);

  removeTempFile();
  deallocateBuffers();

  fprintf(stderr, "Total time: %.3fs\n", getSeconds() - initial_time);
  
  return 0;
}
  



void printHelp() {
  fprintf(stderr,
          "\n  transpose [options]\n"
          "  Read data from standard input, and output a bytewise transpose\n"
          "  of that data to standard output.\n"
          "  Every line in the file must be the same length.\n"
          "  Options:\n"
          "   -m <size> : use this much memory for the buffer\n"
          "               k, m, g suffixes are recognized\n"
          "   -d <directory> : use this directory for temporary files\n"
          "\n");
  exit(1);
}


int parseArgs(int argc, char **argv) {
  int argno;

  memory_size = DEFAULT_MEMORY_SIZE;
  temp_directory = ".";
  input_file = stdin;
  output_file = stdout;


  if (isDirectory("/tmp")) {
    temp_directory = "/tmp";
  }
  
  argno = 1;
  
  while (argno < argc && argv[argno][0] == '-') {
    if (!strcmp(argv[argno], "-")) {
      break;
    }
      
    else if (!strcmp(argv[argno], "-m")) {
      argno++;
      if (argno >= argc) printHelp();
      if (parseSize(argv[argno], &memory_size)) {
        fprintf(stderr, "Invalid memory size: \"%s\"\n", argv[argno]);
        return 1;
      }
    }
      
    else if (!strcmp(argv[argno], "-d")) {
      argno++;
      if (argno >= argc) printHelp();
      if (!fileExists(argv[argno])) {
        fprintf(stderr, "\"%s\" not found\n", argv[argno]);
        return 1;
      }
      if (!isDirectory(argv[argno])) {
        fprintf(stderr, "\"%s\" is not a directory\n", argv[argno]);
        return 1;
      }

      temp_directory = argv[argno];
    }

    else if (!strcmp(argv[argno], "-i")) {
      argno++;
      if (argno >= argc) printHelp();
      input_file = fopen(argv[argno], "r");
      if (!input_file) {
        fprintf(stderr, "Failed to open %s for reading.\n", argv[argno]);
        return 1;
      }
    }

    else if (!strcmp(argv[argno], "-o")) {
      argno++;
      if (argno >= argc) printHelp();
      output_file = fopen(argv[argno], "w");
      if (!input_file) {
        fprintf(stderr, "Failed to open %s for writing.\n", argv[argno]);
        return 1;
      }
    }

    else if (!strcmp(argv[argno], "-h")) {
      printHelp();
    }

    else {
      fprintf(stderr, "Unrecognized option: %s\n", argv[argno]);
      return 1;
    }
      
    argno++;
  }
  
  return 0;
}
  

int readFile(FILE *input_file) {
  char *line_buf = NULL;
  size_t line_buf_capacity = 0;
  ssize_t line_len;
  int result = 0, line_no = 0;

  input_width = -1;
  input_height = -1;

  while (1) {
    line_len = getline(&line_buf, &line_buf_capacity, input_file);
    if (line_len == -1) break;
    
    if (line_no == INT_MAX) {
      fprintf(stderr, "Error: %" PRIu64 " lines read. "
              "Only %d input lines are supported.\n",
              (u64)line_no + 1, INT_MAX);
      goto fail;
    }

    line_len = trimNewline(line_buf, line_len);

    if (line_len > INT_MAX) {
      fprintf(stderr, "Error: input line length %" PRIu64
              " is longer than supported",
              (u64)line_len);
      goto fail;
    }

    if (input_width == -1) {
      /* first line of input. this when we figure out how many columns there
         are in the input. */
      input_width = line_len;
      if (allocateBuffers()) goto fail;
    } else {
      if (line_len != input_width) {
        fprintf(stderr, "Inconsistent line lengths.\n"
                "Line 1 had length %d, but line %d has length %d.\n",
                input_width, line_no, (int)line_len);
        goto fail;
      }
    }

    if (addLine(line_buf, line_no++)) goto fail;
  }

  if (addLine(NULL, line_no)) goto fail;

  goto done;

 fail:
  result = 1;

 done:
  free(line_buf);
  return result;
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


/* Allocate the read buffer, write buffer, and if necessary, 
   the temporary file. */
int allocateBuffers() {
  u64 mem_per_buffer = memory_size / 2;
  int square_size;
  int read_buffer_width, read_buffer_height;

  assert(input_width > 0 && memory_size > 0);

  square_size = (int) sqrt(mem_per_buffer);

  read_buffer_width = MIN(input_width, square_size);
  read_buffer_height = mem_per_buffer / read_buffer_width;
  read_buffer_rows_filled = 0;

  if (Array2d_init(&read_buffer, read_buffer_height, read_buffer_width,
                   read_buffer_width)) {
    fprintf(stderr, "Out of memory: failed to allocate %dx%d read buffer\n",
            read_buffer_height, read_buffer_width);
    return 1;
  }
  fprintf(stderr, "Read buffer %d x %d\n",
          read_buffer_height, read_buffer_width);

  if (Array2d_init(&write_buffer, read_buffer_width, read_buffer_height,
                   read_buffer_height)) {
    fprintf(stderr, "Out of memory: failed to allocate %dx%d write buffer\n",
            read_buffer_width, read_buffer_height);
    return 1;
  }
  fprintf(stderr, "Write buffer %d x %d\n",
          read_buffer_width, read_buffer_height);

  /* don't create a temp file yet */
  
  return 0;
}


void deallocateBuffers() {
  free(read_buffer.data);
  free(write_buffer.data);
}



int insureTempFileExists() {
  if (temp_file) return 0;

  assert(temp_directory && !temp_file_name);

  temp_file_name = strdup(tempnam(temp_directory, "transpose_tmp."));
  fprintf(stderr, "temp file %s\n", temp_file_name);
  
  /*
  temp_file_name = (char*)malloc(strlen(temp_directory) + 22);
  if (!temp_file_name) {
    fprintf(stderr, "Out of memory creating temp file.\n");
    return -1;
  }
  sprintf(temp_file_name, "%s/transpose_tmp.XXXXXX", temp_directory);

  temp_fd = mkostemp(temp_file_name, O_EXCL|O_CREAT);
  if (temp_fd == -1) {
    fprintf(stderr, "Failed to open temp file in 
            temp_file_name);
    return -1;
  }
  */
  
  temp_file = fopen(temp_file_name, "w+");
  if (!temp_file) {
    fprintf(stderr, "Failed to create temp file %s\n", temp_file_name);
    return -1;
  }

  return 0;
}


/* Return the offset in the temp file of the given input row and column.
 */
u64 tempFileOffset(int row, int col) {
  int block_row_idx, block_col_idx;
  int blocks_per_row;
  int row_in_block, col_in_block;
  u64 block_size, block_start, offset;

  assert(read_buffer.n_cols > 0 && read_buffer.n_rows > 0);
  
  block_row_idx = row / read_buffer.n_rows;
  block_col_idx = col / read_buffer.n_cols;

  block_size = (u64)read_buffer.n_cols * read_buffer.n_rows;

  blocks_per_row = (input_width + read_buffer.n_cols - 1) / read_buffer.n_cols;
  
  block_start = block_size * (block_row_idx * blocks_per_row + block_col_idx);

  row_in_block = row % read_buffer.n_rows;
  col_in_block = col % read_buffer.n_cols;
  
  offset = block_start
    + (row_in_block * read_buffer.n_cols)
    + col_in_block;

  return offset;
}  


/* Return the offset in the temp file of the given input row and column.
   The row and column are referencing the output file, and each block
   in the temp file has been transposed.
 */
u64 tempFileOffsetTx(int row, int col) {
  int block_row_idx, block_col_idx;
  int blocks_per_row;
  int row_in_block, col_in_block;
  u64 block_size, block_start, offset;

  assert(read_buffer.n_cols > 0 && read_buffer.n_rows > 0);
  
  block_row_idx = col / read_buffer.n_rows;
  block_col_idx = row / read_buffer.n_cols;

  block_size = (u64)read_buffer.n_cols * read_buffer.n_rows;

  blocks_per_row = (input_width + read_buffer.n_cols - 1) / read_buffer.n_cols;
  
  block_start = block_size * (block_row_idx * blocks_per_row + block_col_idx);

  row_in_block = row % write_buffer.n_rows;
  col_in_block = col % write_buffer.n_cols;
  
  offset = block_start
    + (row_in_block * read_buffer.n_cols)
    + col_in_block;

  return offset;
}  


/* Write to the temp file at the given coordinates. The coordinates are
   in terms of the input file.

   Note that 2-d chunks are contiguous, not rows, and the chunks are the
   size of read_buffer. So if read_buffer is 100x100, byte 101 is
   (row 1, column 1), not (row 0, column 101). */
int tempFileWrite(int row, int col, const char *src, u64 len) {
  u64 offset = tempFileOffset(row, col);
  if (fseek(temp_file, offset, SEEK_SET) ||
      1 != fwrite(src, len, 1, temp_file)) {
    fprintf(stderr, "Failed to write %" PRIu64 " bytes at offset %" PRIu64
            " of %s. Disk full?\n", len, offset, temp_file_name);
    return -1;
  } else {
    return 0;
  }
}


/* Read from the temp file at the given coordinates. The coordinates are
   in terms of the input file. */
int tempFileRead(int row, int col, char *dest, u64 len) {
  u64 offset = tempFileOffset(row, col);
  if (fseek(temp_file, offset, SEEK_SET) ||
      1 != fread(dest, len, 1, temp_file)) {
    fprintf(stderr, "Failed to read %" PRIu64 " bytes at offset %" PRIu64
            " of %s.?\n", len, offset, temp_file_name);
    return -1;
  } else {
    return 0;
  }
}


/* Read from the temp file at the given coordinates. The coordinates are
   in terms of the output file, and the file has been transposed, so the
   blocks are in input order, but within each block the data has been
   transposed. */
int tempFileReadTx(int row, int col, char *dest, u64 len) {
  u64 offset = tempFileOffsetTx(row, col);
  if (fseek(temp_file, offset, SEEK_SET) ||
      1 != fread(dest, len, 1, temp_file)) {
    fprintf(stderr, "Failed to read %" PRIu64 " bytes at offset %" PRIu64
            " of %s.?\n", len, offset, temp_file_name);
    return -1;
  } else {
    return 0;
  }
}


int removeTempFile() {
  if (temp_file_name && temp_file) {
    fclose(temp_file);
    if (remove(temp_file_name)) {
      fprintf(stderr, "Error removing temp file %s: %s\n",
              temp_file_name, strerror(errno));
      return -1;
    }
    free(temp_file_name);
  }
  return 0;
}

int addLine(const char *line, int line_no) {

  /* when this is called with line==NULL, then all data has been read */
  if (line == NULL) {
    input_height = line_no;

    if (temp_file) {
      /* flush the buffer, if a temp file has been created */
      if (read_buffer_rows_filled) {
        if (tempFileWrite(line_no - read_buffer_rows_filled,
                          0, read_buffer.data,
                          (u64)read_buffer_rows_filled * read_buffer.n_cols))
        return -1;
        read_buffer_rows_filled = 0;
      } else {
        u64 block_count, block_size, file_len;
        /* If individual lines were being written to the file, the final
           block may be incomplete. Make sure the block is complete, even
           if the unneeded data at the end is all zeros. */
        block_size = (u64)read_buffer.n_rows * read_buffer.n_cols;
        block_count =
          (input_width + read_buffer.n_cols - 1) / read_buffer.n_cols;
        block_count *=
          (input_height + read_buffer.n_rows - 1) / read_buffer.n_rows;
        file_len = block_size * block_count;
        if (ftruncate(fileno(temp_file), file_len)) {
          fprintf(stderr, "Failed to set length of %s to %" PRIu64
                  " (out of disk space?)\n", temp_file_name, file_len);
          return -1;
        }
      }
    }
    return 0;
  }

  assert(strlen(line) == input_width);
  
  /* If input lines fit in the input buffer, add this line to the buffer */
  if (input_width <= read_buffer.n_cols) {

    /* the buffer is full, flush it to the temp file */
    if (read_buffer_rows_filled == read_buffer.n_rows) {
      if (insureTempFileExists()) return -1;
      if (tempFileWrite(line_no - read_buffer_rows_filled, 0, read_buffer.data,
                        (u64)read_buffer_rows_filled * read_buffer.n_cols))
        return -1;
      read_buffer_rows_filled = 0;
    }
    
    memcpy(Array2d_ptr(&read_buffer, read_buffer_rows_filled, 0),
           line, input_width);
    read_buffer_rows_filled++;

  } else {
    int line_block_col, block_len;
  
    if (insureTempFileExists()) return -1;
    
    /* copy each chunk of the line to the temp file */
    for (line_block_col = 0; line_block_col < input_width;
         line_block_col += read_buffer.n_cols) {
      block_len = MIN(read_buffer.n_cols, input_width - line_block_col);
      if (tempFileWrite(line_no, line_block_col, line + line_block_col,
                        block_len))
        return -1;
    }
  }
  return 0;
}


/* If the data is all in the read buffer, just transpose it into the write
   buffer. 

   If it's in the temp file, scan through the temp file, transposing each
   block in place.
*/
int transposeData() {
  int row, col, height, width;
  u64 nbytes = (u64)read_buffer.n_rows * read_buffer.n_cols;

  /* if there's a temp file, transpose every block in it */
  if (temp_file) {
    for (row=0; row < input_height; row += read_buffer.n_rows) {
      height = MIN(read_buffer.n_rows, input_height - row);
      for (col=0; col < input_width; col += read_buffer.n_cols) {
        width = MIN(read_buffer.n_cols, input_width - col);

#if VERBOSE > 1
        fprintf(stderr, "Transpose block at (%d,%d)\n", row, col);
#endif
        if (tempFileRead(row, col, read_buffer.data, nbytes))
          return -1;

        transpose(&write_buffer, 0, 0, &read_buffer, 0, 0,
                  height, width);

        if (tempFileWrite(row, col, write_buffer.data, nbytes))
          return -1;
      }
    }
  }

  /* the data fit in the read buffer, transpose it into the write buffer */
  else {
    transpose(&write_buffer, 0, 0, &read_buffer, 0, 0,
              read_buffer.n_rows, read_buffer.n_cols);
  }

  return 0;
}


/* If the data is all in the write buffer, just write it to stdout.

   If it's in the temp file, splice together each line of output and
   write them to stdout.
*/
int writeFile(FILE *output_file) {
  int row, col, width, result = 0;
  char *output_row = (char*) malloc(input_height + 2);
  if (!output_row) {
    fprintf(stderr, "Out of memory: failed to allocate output row buffer\n");
    return -1;
  }
  strcpy(output_row + input_height, "\n");

  if (temp_file) {
    /* loop through the output rows */
    for (row=0; row < input_width; row++) {

      /* read each chunk of the line */
      for (col=0; col < input_height; col += read_buffer.n_rows) {
        width = MIN(read_buffer.n_cols, input_height - col);

        if (tempFileReadTx(row, col, output_row + col, width)) goto fail;
      }

      if (1 != fwrite(output_row, input_height + 1, 1, output_file)) {
        fprintf(stderr, "Failed to write output row %d (disk full?)\n", row);
        goto fail;
      }
    }

  } else {
    /* just write out the write buffer */
    for (row=0; row < input_width; row++) {
      memcpy(output_row, Array2d_ptr(&write_buffer, row, 0), input_height);
      if (1 != fwrite(output_row, input_height + 1, 1, output_file)) {
        fprintf(stderr, "Failed to write output row %d (disk full?)\n", row);
        goto fail;
      }
    }
  }

  goto done;
  
 fail:
  result = -1;
 done:
  free(output_row);
  return result;
}

