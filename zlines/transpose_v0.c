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
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <limits.h>
#include <sys/stat.h>
#include <sys/types.h>

#ifndef _WIN32
#include <unistd.h>
#endif

#include "common.h"

typedef uint64_t u64;

#define STATUS_REPORT_BYTE_INCREMENT (10*1000*1000)
#define VERBOSE 0

#ifndef DEFAULT_TILE_SIZE
#define DEFAULT_TILE_SIZE (16*1024)
#endif

#define USE_CACHE_OBLIVIOUS_TX 1
#define ROW_MAJOR 0


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

int newline_type;

void printHelp();

/* Transpose a 2d array. This will call one of the other transpose routines,
   depending on the size of the array. */
void transpose
(Array2d *dest, int dest_row, int dest_col,
 Array2d *src, int src_row, int src_col,
 int height, int width);

void transposeFiles(File2d *out, File2d *in, int tile_size);

void transposeFileBlocks
(File2d *dest, int dest_row, int dest_col,
 File2d *src, int src_row, int src_col,
 int height, int width);

void transposeFileRecursive
(File2d *dest, int dest_row, int dest_col,
 File2d *src, int src_row, int src_col,
 int height, int width);

/* copy one tile from src to dest, transposing it along the way */
void transposeFileTile(File2d *dest, int dest_row, int dest_col,
                       File2d *src, int src_row, int src_col,
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
  in.n_rows = out.n_cols = in.n_cols = out.n_rows = in.row_stride =
    tile_size;

  /* allocate an extra column or two for 'out', so it can efficiently write
     newlines to the output file */
  out.row_stride = tile_size + newlineLength(out_file->newline_type);

  /* allocate data for an input buffer and output buffer */
  in.data = (char*)malloc(in.n_rows * in.row_stride);
  out.data = (char*)malloc(out.n_rows * out.row_stride);
  assert(in.data && out.data);

#if USE_CACHE_OBLIVIOUS_TX
  transposeFileRecursive
#else
  transposeFileBlocks
#endif
    (out_file, 0, 0, in_file, 0, 0,
     in_file->n_rows, in_file->n_cols);

  free(in.data);
  free(out.data);

  statusReport(1);
  printf("read time %.3fs, in-memory transpose time %.3fs, "
         "write time %.3fs\n",
         time_reading, time_transposing, time_writing);
}


void transposeFileBlocks(File2d *dest, int dest_row, int dest_col,
                         File2d *src, int src_row, int src_col,
                         int height, int width) {
  int row, col, block_width, block_height;
  
  /* read blocks of the input file, transpose them, and write blocks to
     the output file. */
#if ROW_MAJOR
  for (row = 0; row < height; row += in.n_rows) {
    block_height = MIN(in.n_rows, src->n_rows - row);
    for (col = 0; col < width; col += in.n_cols) {
      block_width = MIN(in.n_cols, src->n_cols - col);
#else
  for (col = 0; col < width; col += in.n_cols) {
    block_width = MIN(in.n_cols, src->n_cols - col);
    for (row = 0; row < height; row += in.n_rows) {
      block_height = MIN(in.n_rows, src->n_rows - row);
#endif

      transposeFileTile(dest, col, row, src, row, col,
                        block_height, block_width);
      
    }
  }
}


void transposeFileRecursive(File2d *dest, int dest_row, int dest_col,
                            File2d *src, int src_row, int src_col,
                            int height, int width) {

  if (height > tile_size || width > tile_size) {
    int half;
    if (height > width) {
      half = height / 2;
      transposeFileRecursive(dest, dest_row, dest_col,
                             src, src_row, src_col,
                             half, width);
      transposeFileRecursive(dest, dest_row, dest_col+half,
                             src, src_row+half, src_col,
                             height-half, width);
    } else {
      half = width / 2;
      transposeFileRecursive(dest, dest_row, dest_col,
                             src, src_row, src_col,
                             height, half);
      transposeFileRecursive(dest, dest_row+half, dest_col,
                             src, src_row, src_col+half,
                             height, width-half);
    }
    return;
  }      

  transposeFileTile(dest, dest_row, dest_col,
                    src, src_row, src_col, height, width);

}


void transposeFileTile(File2d *dest, int dest_row, int dest_col,
                       File2d *src, int src_row, int src_col,
                       int height, int width) {
  double start_time;
  int row, tmp;

  /*
  printf("block %d,%d size %d x %d to %d,%d\n", src_row, src_col,
         height, width, dest_row, dest_col);
  */
      
  /* read a block into input buffer */
  copy2dFromFile(&in, 0, 0, src, src_row, src_col, height, width);

  /* transpose that block into the output buffer */
  start_time = getSeconds();
  transpose(&out, 0, 0, &in, 0, 0, height, width);
  time_transposing += getSeconds() - start_time;

  bytes_done += (u64)width * height;

  tmp = height; height = width; width = tmp;

  /* if this is the last block of the row, write the newlines as well. */
  if (dest_col + width == dest->n_cols) {
    for (row = 0; row < height; row++)
      writeNewline(Array2d_ptr(&out, row, width), dest->newline_type);
      
    width += newlineLength(dest->newline_type);
  }

  /* write the output buffer to the output file */
  copy2dToFile(dest, dest_row, dest_col, &out, 0, 0, height, width);
  tiles_done++;
#if VERBOSE > 1
  printf("  write %.3fs\n", elapsed);
#endif

  statusReport(0);
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
  double start_time = getSeconds();

  for (row = 0; row < height; row++) {
    file_offset = File2d_offset(dest, dest_row + row, dest_col);
    if (width != pwrite(dest->fd, Array2d_ptr(src, src_row + row, src_col),
                        width, file_offset)) {
      fprintf(stderr, "Error writing %d bytes to %s at offset %" PRIu64 "\n",
              width, dest->filename, file_offset);
    }
  }

  time_writing += getSeconds() - start_time;
}
  

void copy2dFromFile
(Array2d *dest, int dest_row, int dest_col,
 File2d *src, int src_row, int src_col,
 int height, int width) {
  int row;
  u64 file_offset;
  double start_time = getSeconds();

  for (row = 0; row < height; row++) {
    file_offset = File2d_offset(src, src_row + row, src_col);
    if (width != pread(src->fd, Array2d_ptr(dest, dest_row + row, dest_col),
                        width, file_offset)) {
      fprintf(stderr, "Error read %d bytes from %s at offset %" PRIu64 "\n",
              width, src->filename, file_offset);
    }
  }

  time_reading += getSeconds() - start_time;
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
  
  printf("\r%.2f%% of %s bytes done"
         /* ", r=%.3f tx=%.3f w=%.3f"*/
         , pct_done, commafy(buf1, byte_count)
         /* , tiles_done, tile_count */
         /*, time_reading, time_transposing, time_writing */
         );
         
  if (final) putchar('\n');
  fflush(stdout);
  next_report += STATUS_REPORT_BYTE_INCREMENT;
}
