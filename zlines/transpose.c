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

  Performance snapshot, 2015-01-25:
% ./transpose run1.chars /data/bioio/run1.chars.t
run1.chars has 1999999 rows of length 21946 with unix line endings
temp buffer 4096x4096
43,800,008,320 of 43,891,978,054 bytes done
transpose 1999999x21946 = 43891978054 bytes in 1084.340s at 38.6 MiB/s

  source drive: SSD, destination hard: HDD


  Ed Karrels, edk@illinois.edu, January 2017
*/

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <limits.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include "common.h"

typedef uint64_t u64;

#define STATUS_REPORT_BYTE_INCREMENT (100*1000*1000)
#define DEFAULT_TILE_SIZE (4 * 1024)
#define VERBOSE 0


typedef struct {
  int fd;
  const char *filename;
  int n_rows, n_cols, row_stride, newline_type;
} File2d;

/* If for_writing is nonzero, n_rows, n_cols, row_stride, and newline_type
   will need to be set before calling this. If it's zero, then they will
   be filled in. */
int File2d_open(File2d *f, const char *filename, int for_writing);
void File2d_close(File2d *f);

/* Find the first instance of the given byte in the file */
int File2d_find_byte(File2d *f, char c);

/* Get the offset of a location in a File2d */
u64 File2d_offset(File2d *f, int row, int col);

File2d in_file, out_file;
Array2d in, out;
double time_reading=0, time_transposing=0, time_writing=0;
int tile_count, tiles_done=0;
u64 bytes_done=0, byte_count;

/* If nonzero, blocks read will progress to the right, then down, and
   blocks written will progress down, then right.
   If zero, the the opposite. */
int move_right = 0;

/* The data is processed in blocks. This is the number of rows in 
   each block read and number of columns in each block written. */
int read_block_height = 256;

/* Number of columns in each block read and number of rows
   in each block written. */
int read_block_width = 256;

int cache_ob_size = 128;

int tile_size = DEFAULT_TILE_SIZE;

#define NEWLINE_UNIX 1
#define NEWLINE_DOS 2

int newline_type;

void printHelp();
#define newlineLength(newline_type) (newline_type)
#define newlineName(newline_type) ((newline_type)==(NEWLINE_DOS)?"DOS":"unix")
void writeNewline(char *dest, int newline_type);

/* Transpose a 2d array. This will call one of the other transpose routines,
   depending on the size of the array. */
void transpose
(Array2d *dest, int dest_row, int dest_col,
 Array2d *src, int src_row, int src_col,
 int height, int width);

void transposeFiles(File2d *out, File2d *in, int tile_size);

/* transpose the data using a block algorithm. */
void transposeBlocks
(Array2d *dest, int dest_row, int dest_col,
 Array2d *src, int src_row, int src_col,
 int height, int width);

/* simple copy without transposing */
void copy2d
(Array2d *dest, int dest_row, int dest_col,
 Array2d *src, int src_row, int src_col,
 int height, int width);

void copy2dToFile
(File2d *dest, int dest_row, int dest_col,
 Array2d *src, int src_row, int src_col,
 int height, int width);

void copy2dFromFile
(Array2d *dest, int dest_row, int dest_col,
 File2d *src, int src_row, int src_col,
 int height, int width);

int do_status_reports = 1;
void statusReport(int final);


int main(int argc, char **argv) {
  int result = 0;
  /* int newline_len; */
  double start_time, elapsed, mbps;

  if (argc != 3) printHelp();

  if (File2d_open(&in_file, argv[1], 0)) {
    fprintf(stderr, "Failed to open %s\n", argv[1]);
    return 1;
  }

  printf("%s has %d rows of length %d with %s line endings\n",
         in_file.filename, in_file.n_rows, in_file.n_cols,
         newlineName(in_file.newline_type));

  out_file.n_rows = in_file.n_cols;
  out_file.n_cols = in_file.n_rows;
  out_file.newline_type = in_file.newline_type;

  if (File2d_open(&out_file, argv[2], 1)) {
    fprintf(stderr, "Failed to open %s\n", argv[2]);
    return 1;
  }

  /* only print status reports if stdout is a terminal */
  do_status_reports = isatty(1);
  
  start_time = getSeconds();

  byte_count = (u64)in_file.n_rows * in_file.n_cols;
  /* printf("%" PRIu64 " bytes of memory\n", getMemorySize()); */
  tile_size = DEFAULT_TILE_SIZE;
  tile_count = ((in_file.n_cols + tile_size - 1) / tile_size)
    * ((in_file.n_rows + tile_size - 1) / tile_size);
  
  transposeFiles(&out_file, &in_file, tile_size);

  File2d_close(&in_file);

  /* the close time can be significant */
  File2d_close(&out_file);

  elapsed = getSeconds() - start_time;
  mbps = byte_count / (elapsed * 1024 * 1024);
  printf("transpose %dx%d = %" PRIu64" bytes in %.3fs at %.1f MiB/s\n",
         in_file.n_rows, in_file.n_cols, byte_count, elapsed, mbps);
  
  return result;
}


void printHelp() {
  printf("\n  transpose <input_file> <output_file>\n"
         "  Do a bytewise transpose of the lines of the given file.\n"
         "  Every line in the file must be the same length.\n\n");
  exit(1);
}


/* Get the offset of a location in a File2d */
u64 File2d_offset(File2d *f, int row, int col) {
  return (u64)row * f->row_stride + col;
}


void writeNewline(char *dest, int newline_type) {
  if (newline_type == NEWLINE_DOS) {
    dest[0] = '\r';
    dest[1] = '\n';
  } else {
    dest[0] = '\n';
  }
}


int createTempBuffer(Array2d *temp, int tile_size) {
  u64 temp_size = (u64)tile_size * tile_size;

  assert(in.n_cols > 0 && in.n_rows > 0);

  if (in.n_cols < tile_size) {
    temp->n_rows = in.n_cols;
    temp->n_cols = MIN(in.n_rows, temp_size / in.n_cols);
  } else if (in.n_rows < tile_size) {
    temp->n_cols = in.n_rows;
    temp->n_rows = MIN(in.n_cols, temp_size / in.n_rows);
  } else {
    temp->n_rows = tile_size;
    temp->n_cols = tile_size;
  }

  printf("temp buffer %dx%d\n", temp->n_rows, temp->n_cols);
  temp->row_stride = temp->n_cols;
  
  temp->data = (char*) malloc((u64)temp->n_cols * temp->n_rows);
  if (!temp->data) {
    fprintf(stderr, "Failed to allocate %dx%d temp buffer.\n",
            temp->n_rows, temp->n_cols);
    return 1;
  }

  return 0;
}


void transposeFiles(File2d *out_file, File2d *in_file, int tile_size) {
  Array2d in, out;
  int row, x, y, block_width, block_height;
  double start_time, elapsed;

  in.n_rows = out.n_cols = in.n_cols = out.n_rows = in.row_stride =
    tile_size;

  /* allocate an extra column or two for 'out', so it can efficiently write
     newlines to the output file */
  out.row_stride = tile_size + newlineLength(out_file->newline_type);

  /* allocate data for an input buffer and output buffer */
  in.data = (char*)malloc(in.n_rows * in.row_stride);
  out.data = (char*)malloc(out.n_rows * out.row_stride);
  assert(in.data && out.data);

  /* read blocks of the input file, transpose them, and write blocks to
     the output file. */
  for (x = 0; x < in_file->n_cols; x += in.n_cols) {
    block_width = MIN(in.n_cols, in_file->n_cols - x);

    for (y = 0; y < in_file->n_rows; y += in.n_rows) {
      block_height = MIN(in.n_rows, in_file->n_rows - y);

      /* read a block into input buffer */
      start_time = getSeconds();
      copy2dFromFile(&in, 0, 0, in_file, y, x, block_height, block_width);
      elapsed = getSeconds() - start_time;
      time_reading += elapsed;
#if VERBOSE > 1
      printf("read %dx%d block %.3fs\n", block_height, block_width, elapsed);
#endif

      /* transpose that block into the output buffer */
      start_time = getSeconds();
      transpose(&out, 0, 0, &in, 0, 0, block_height, block_width);
      elapsed = getSeconds() - start_time;
      time_transposing += elapsed;
#if VERBOSE > 1
      printf("  transpose %.3fs\n", elapsed);
#endif

      /* if this is the last block of the row, write the newlines as well. */
      if (y + block_height == out_file->n_cols) {
        for (row = 0; row < block_width; row++)
          writeNewline(Array2d_ptr(&out, row, block_height),
                       out_file->newline_type);
      
        block_height += newlineLength(out_file->newline_type);
      }

      /* write the output buffer to the output file */
      start_time = getSeconds();
      copy2dToFile(out_file, x, y, &out, 0, 0, block_width, block_height);
      elapsed = getSeconds() - start_time;
      time_writing += elapsed;
      tiles_done++;
#if VERBOSE > 1
      printf("  write %.3fs\n", elapsed);
#endif

      statusReport(0);

    }
  }

  free(in.data);
  free(out.data);

  statusReport(1);
  printf("read time %.3fs, in-memory transpose time %.3fs, "
         "write time %.3fs\n",
         time_reading, time_transposing, time_writing);
}


/* height and width are in terms of the src array */
void transposeBlocks(Array2d *dest, int dest_row, int dest_col,
                     Array2d *src, int src_row, int src_col,
                     int height, int width) {

  int x, y, block_width, block_height;
  
  for (x = 0; x < width; x += read_block_width) {
    block_width = MIN(read_block_width, width - x);

    for (y = 0; y < height; y += read_block_height) {
      block_height = MIN(read_block_height, height - y);

      transposeTile(dest, dest_row + x, dest_col + y,
                    src, dest_row + y, src_col + x,
                    block_height, block_width);

    }
  }

}

                
/* simple copy without transposing */
void copy2d
(Array2d *dest, int dest_row, int dest_col,
 Array2d *src, int src_row, int src_col,
 int height, int width) {

  int row;

  for (row = 0; row < height; row++) {
    memcpy(Array2d_ptr(dest, dest_row + row, dest_col),
           Array2d_ptr(src, src_row + row, src_col),
           width);

    if (dest == &out && dest_col + width == dest->n_cols)
      writeNewline(Array2d_ptr(dest, dest_row + row, dest->n_cols),
                   newline_type);
  }
}


void copy2dToFile
(File2d *dest, int dest_row, int dest_col,
 Array2d *src, int src_row, int src_col,
 int height, int width) {
  int row;
  u64 file_offset;

  for (row = 0; row < height; row++) {
    file_offset = File2d_offset(dest, dest_row + row, dest_col);
    if (width != pwrite(dest->fd, Array2d_ptr(src, src_row + row, src_col),
                        width, file_offset)) {
      fprintf(stderr, "Error writing %d bytes to %s at offset %" PRIu64 "\n",
              width, dest->filename, file_offset);
    }
  }
}
  

void copy2dFromFile
(Array2d *dest, int dest_row, int dest_col,
 File2d *src, int src_row, int src_col,
 int height, int width) {
  int row;
  u64 file_offset;

  for (row = 0; row < height; row++) {
    file_offset = File2d_offset(src, src_row + row, src_col);
    if (width != pread(src->fd, Array2d_ptr(dest, dest_row + row, dest_col),
                        width, file_offset)) {
      fprintf(stderr, "Error read %d bytes from %s at offset %" PRIu64 "\n",
              width, src->filename, file_offset);
    }
  }
}

int File2d_open(File2d *f, const char *filename, int for_writing) {
  int flags;
  mode_t mode = 0;
  u64 length, first_line_len, row;
  char buf[10];

  if (for_writing) {
    flags = O_RDWR | O_CREAT | O_TRUNC;
    mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
    assert(f->n_cols && f->n_rows && f->newline_type);
    f->row_stride = f->n_cols + newlineLength(f->newline_type);
  } else {
    flags = O_RDONLY;
    length = getFileSize(filename);
    if (length == 0) goto fail;
  }
  
  f->filename = filename;
  f->fd = open(filename, flags, mode);
  if (f->fd == -1) goto fail;

  if (for_writing) {
    length = (u64)f->n_rows * f->row_stride;
    if (ftruncate(f->fd, length)) {
      fprintf(stderr, "Failed to set the length of %s to %" PRIu64 " bytes\n",
              filename, length);
      goto fail;
    }
  } else {
    assert(length > 0);

    /* guesstimate out the file dimensions */
    first_line_len = File2d_find_byte(f, '\n');
    if (first_line_len >= INT_MAX) {
      fprintf(stderr, "First line of %s is %" PRIu64 " bytes, which is longer "
              "than this tool supports.\n", filename, first_line_len);
      goto fail;
    }

    if (first_line_len == length || first_line_len == -1) {
      fprintf(stderr, "Invalid input file: no line endings found.\n");
      goto fail;
    }
      

    if (first_line_len == 0) {
      fprintf(stderr, "Invalid input file: first line is empty\n");
      goto fail;
    }

    /* check for DOS (0x0d 0x0a) line endings, since having two bytes at the
       end of each line will change the row stride of the data */
    if (1 != pread(f->fd, buf, 1, first_line_len-1)) {
      fprintf(stderr, "Input file error: read failed\n");
      goto fail;
    }
    f->newline_type = (buf[0] == '\r') ? NEWLINE_DOS : NEWLINE_UNIX;

    f->row_stride = first_line_len + 1;
    f->n_cols = f->row_stride - newlineLength(f->newline_type);

    /* For this tool to work correctly, every line of the file must have
       the same length. Don't check every line, but check that the file
       size is an integer multiple of the line length. */
    
    f->n_rows = length / f->row_stride;
    if (length != f->n_rows * (u64)f->row_stride) {
      fprintf(stderr, "Invalid input file: uneven line lengths "
              "(rows appear to be %d bytes each, but that doesn't evenly "
              "divide the file length, %" PRIu64 "\n",
              f->row_stride, length);
      goto fail;
    }

    /* Check for the presence of a newline character in the right place
       in a few more rows. */

    for (row=10; row < f->n_rows; row *= 10) {
      /* printf("check row %d\n", (int)row); */
      if (1 != pread(f->fd, buf, 1, File2d_offset(f, row, f->row_stride-1))) {
        fprintf(stderr, "Input file error: read failed\n");
        goto fail;
      }
      if (buf[0] != '\n') {
        fprintf(stderr, "Invalid input file: row %d length mismatch\n",
                (int)row);
        return -1;
      }
    }
    
  }

  return 0;

 fail:
  if (f->fd >= 0) close(f->fd);
  f->fd = -1;
  return -1;
}
    

/* Find the first instance of the given byte in the file */
int File2d_find_byte(File2d *f, char c) {
  char buf[4096], *found_pos;
  u64 pos, original_pos =  lseek(f->fd, 0, SEEK_CUR);
  int read_len;

  pos = lseek(f->fd, 0, SEEK_SET);
  assert(pos == 0);

  do {
    read_len = read(f->fd, buf, sizeof buf);
    if (read_len < 0) return -1;
    found_pos = memchr(buf, c, read_len);
    if (found_pos) {
      lseek(f->fd, original_pos, SEEK_SET);
      return pos + (found_pos - buf);
    }
    pos += read_len;
  } while (read_len == sizeof buf);

  lseek(f->fd, original_pos, SEEK_SET);
  return -1;
}
  
  
void File2d_close(File2d *f) {
  close(f->fd);
  f->fd = -1;
}


/* Call this to print a status update occasionally.
   It reads these global variables:
     tile_count
     tiles_done
     in_file.n_rows
     in_file.n_cols
     bytes_done
     time_reading
     time_transposing
     time_writing
   If 'final' is nonzero, all the processing is done, so print at final
   status report with a trailing newline.
*/
void statusReport(int final) {
  static u64 next_report = 0;
  char buf1[50];
  double pct_done;
  
  if (!do_status_reports) return;
  if (!final && bytes_done < next_report) return;

  pct_done = 100.0 * bytes_done / byte_count;
  
  printf("\r%.2f%% of %s bytes done, %d of %d tiles, r=%.3f tx=%.3f w=%.3f",
         pct_done, commafy(buf1, byte_count), tiles_done, tile_count,
         time_reading, time_transposing, time_writing);
  if (final) putchar('\n');
  fflush(stdout);
  next_report += STATUS_REPORT_BYTE_INCREMENT;
}
