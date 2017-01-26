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


typedef struct {
  char *data;
  int n_rows, n_cols, row_stride;
} Array2d;

/* Get a pointer to a character in an Array2d. 'a' should be an Array2d,
   not a pointer to one. */
#define Array2d_ptr(a, row, col) ((a).data + (u64)(row) * (a).row_stride + (col))

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

int default_temp_size = 4096;

#define NEWLINE_UNIX 1
#define NEWLINE_DOS 2

int newline_type;

#define MIN(a,b) ((a)<(b)?(a):(b))
#define MAX(a,b) ((a)>(b)?(a):(b))

void printHelp();
int getFileDimensions(Array2d *array, u64 length, int *newline_type);
#define newlineLength(newline_type) (newline_type)
#define newlineName(newline_type) ((newline_type)==(NEWLINE_DOS)?"DOS":"unix")
void writeNewline(char *dest, int newline_type);
int createTempBuffer(Array2d *temp);

/* Transpose a 2d array. This will call one of the other transpose routines,
   depending on the size of the array. */
void transpose
(Array2d *dest, int dest_row, int dest_col,
 Array2d *src, int src_row, int src_col,
 int height, int width);

void transposeFiles(File2d *out, File2d *in);

/* Transpose large chunks from the input file into memory, then copy them
   to the output file. */
void transposeTwoStage
(Array2d *dest, int dest_row, int dest_col,
 Array2d *src, int src_row, int src_col,
 int height, int width);

/* transpose the data using a block algorithm. */
void transposeBlocks
(Array2d *dest, int dest_row, int dest_col,
 Array2d *src, int src_row, int src_col,
 int height, int width);

/* transpose the data using a cache-oblivious algorithm. */
void transposeCacheOblivious
(Array2d *dest, int dest_row, int dest_col,
 Array2d *src, int src_row, int src_col,
 int height, int width);

/* Transpose one tile using a simple algorithm. */
void transposeTile
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



int main(int argc, char **argv) {
  int result = 0;
  /* int newline_len; */
  u64 /* out_file_len = 0,*/ byte_count;
  double start_time, elapsed, mbps;

  if (argc != 3) printHelp();

  if (File2d_open(&in_file, argv[1], 0)) {
    fprintf(stderr, "Failed to open %s\n", argv[1]);
    return 1;
  }
    
  /*
  if (mapFile(argv[1], 0, (char**)&in.data, &in_file_len)) {
    fprintf(stderr, "Failed to open %s\n", argv[1]);
    return 1;
  }

  if (getFileDimensions(&in, in_file_len, &newline_type))
    goto fail;
  */

  printf("%s has %d rows of length %d with %s line endings\n",
         in_file.filename, in_file.n_rows, in_file.n_cols,
         newlineName(in_file.newline_type));

  out_file.n_rows = in_file.n_cols;
  out_file.n_cols = in_file.n_rows;
  out_file.newline_type = in_file.newline_type;

  /* out_file_len = (u64)out.n_rows * out.row_stride; */

  /*
  if (mapFile(argv[2], 1, &out.data, &out_file_len)) {
    fprintf(stderr, "Failed to open %s\n", argv[2]);
    return 1;
  }
  */

  if (File2d_open(&out_file, argv[2], 1)) {
    fprintf(stderr, "Failed to open %s\n", argv[2]);
    return 1;
  }

  start_time = getSeconds();

  byte_count = (u64)in_file.n_rows * in_file.n_cols;

  transposeFiles(&out_file, &in_file);
  /* transposeTwoStage(&out, 0, 0, &in, 0, 0, in.n_rows, in.n_cols); */

  putchar('\n');
  
  elapsed = getSeconds() - start_time;
  mbps = byte_count / (elapsed * 1024 * 1024);
  printf("transpose %dx%d = %" PRIu64" bytes in %.3fs at %.1f MiB/s\n",
         in_file.n_rows, in_file.n_cols, byte_count, elapsed, mbps);

  /*
 fail:
  if (in.data) munmap(in.data, in_file_len);
  if (out.data) munmap(out.data, out_file_len);
  */
  
  return result;
}


void printHelp() {
  printf("\n  transpose <input_file> <output_file>\n"
         "  Do a bytewise transpose of the lines of the given file.\n"
         "  Every line in the file must be the same length.\n\n");
  exit(1);
}


int getFileDimensions(Array2d *array, u64 length, int *newline_type) {
  const char *data, *first_newline;
  u64 row;

  data = array->data;
  
  /* find the end of the first line of the file */
  first_newline = strchr(data, '\n');
  if (!first_newline) {
    fprintf(stderr, "Invalid input file: no line endings found\n");
    return -1;
  }

  if (first_newline == data) {
    fprintf(stderr, "Invalid input file: first line is empty\n");
    return -1;
  }

  /* check for DOS (0x0d 0x0a) line endings, since having two bytes at the
     end of each line will change the row stride of the data */
  if (first_newline[-1] == '\r') {
    *newline_type = NEWLINE_DOS;
  } else {
    *newline_type = NEWLINE_UNIX;
  }
  array->row_stride = (first_newline - data) + 1;
  array->n_cols = array->row_stride - newlineLength(*newline_type);

  /* For this tool to work correctly, every line of the file must have
     the same length. Don't check every line, but check that the file
     size is an integer multiple of the line length. */

  array->n_rows = length / array->row_stride;
  if (length != array->n_rows * (u64)array->row_stride) {
    fprintf(stderr, "Invalid input file: uneven line lengths "
            "(rows appear to be %d bytes each, but that doesn't evenly "
            "divide the file length, %" PRIu64 "\n",
            array->row_stride, length);
    return -1;
  }

  /* Check for the presence of a newline character in the right place
     in a few more rows. */

  for (row=10; row < array->n_rows; row *= 10) {
    /* printf("check row %d\n", (int)row); */
    if (data[row * array->row_stride - 1] != '\n') {
      fprintf(stderr, "Invalid input file: row %d length mismatch\n", (int)row);
      return -1;
    }
  }

  return 0;
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


void writeTileNewlines(int block_top, int block_bottom) {
  int row;
  for (row = block_top; row < block_bottom; row++)
    writeNewline(Array2d_ptr(out, row, out.n_cols), newline_type);
}


int createTempBuffer(Array2d *temp) {
  u64 temp_size = (u64)default_temp_size * default_temp_size;

  assert(in.n_cols > 0 && in.n_rows > 0);

  if (in.n_cols < default_temp_size) {
    temp->n_rows = in.n_cols;
    temp->n_cols = MIN(in.n_rows, temp_size / in.n_cols);
  } else if (in.n_rows < default_temp_size) {
    temp->n_cols = in.n_rows;
    temp->n_rows = MIN(in.n_cols, temp_size / in.n_rows);
  } else {
    temp->n_rows = default_temp_size;
    temp->n_cols = default_temp_size;
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


/* Transpose a 2d array. This will call one of the other transpose routines,
   depending on the size of the array. */
void transpose
(Array2d *dest, int dest_row, int dest_col,
 Array2d *src, int src_row, int src_col,
 int height, int width) {

  if (height <= read_block_height &&
      width <= read_block_width) {
    transposeTile(dest, dest_row, dest_col, src, src_row, src_col,
                  height, width);
  } else {

    u64 byte_count = (u64) height * width;

    if (byte_count < 100*1024*1024) {

      /* choose either by blocks or with the cache-oblivious algorithm */
      
      transposeCacheOblivious
        /* transposeBlocks */
        (dest, dest_row, dest_col, src, src_row, src_col,
         height, width);

    } else {
      /* For really big arrays, first transpose large chunks from the
         input file into memory, then copy them to the output file. */
      transposeTwoStage(dest, dest_row, dest_col, src, src_row, src_col,
                        height, width);
    }
  }
}


void transposeFiles(File2d *out_file, File2d *in_file) {
  Array2d in, out;
  int row, x, y, block_width, block_height;
  double start_time, elapsed;

  in.n_rows = out.n_cols = in.n_cols = out.n_rows = in.row_stride =
    default_temp_size;

  /* allocate an extra column or two for 'out', so it can efficiently write
     newlines to the output file */
  out.row_stride = default_temp_size + newlineLength(out_file->newline_type);

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
      /* printf("read %dx%d block %.3fs\n", block_height, block_width, elapsed); */

      /* transpose that block into the output buffer */
      start_time = getSeconds();
      transpose(&out, 0, 0, &in, 0, 0, block_height, block_width);
      elapsed = getSeconds() - start_time;
      time_transposing += elapsed;
      /* printf("  transpose %.3fs\n", elapsed); */

      /* if this is the last block of the row, write the newlines as well. */
      if (y + block_height == out_file->n_cols) {
        for (row = 0; row < block_width; row++)
          writeNewline(Array2d_ptr(out, row, block_height),
                       out_file->newline_type);
      
        block_height += newlineLength(out_file->newline_type);
      }

      /* write the output buffer to the output file */
      start_time = getSeconds();
      copy2dToFile(out_file, x, y, &out, 0, 0, block_width, block_height);
      elapsed = getSeconds() - start_time;
      time_writing += elapsed;
      /* printf("  write %.3fs\n", elapsed); */

    }
  }

  free(in.data);
  free(out.data);

  printf("\nread time %.3fs, in-memory transpose time %.3fs, write time %.3fs\n",
         time_reading, time_transposing, time_writing);
}
  
  


/* Transpose large chunks from the input file into memory, then copy them
   to the output file. */
void transposeTwoStage
(Array2d *dest, int dest_row, int dest_col,
 Array2d *src, int src_row, int src_col,
 int height, int width) {
  Array2d temp = {0};

  if (createTempBuffer(&temp)) exit(1);

  int x, y, block_width, block_height;
  
  for (x = 0; x < width; x += temp.n_cols) {
    block_width = MIN(temp.n_cols, width - x);

    for (y = 0; y < height; y += temp.n_rows) {
      block_height = MIN(temp.n_rows, height - y);
      /*
      printf("transpose big tile (%d,%d) - (%d,%d)\n",
             src_row + y, src_col + x,
             src_row + y + block_height, src_col + x + block_width);
      */
      /* copy from the input file to temp, transposing along the way */
      transposeCacheOblivious
        (&temp, 0, 0,
         src, src_row + y, src_col + x,
         block_height, block_width);
      /*
      printf("write big tile to (%d,%d) - (%d,%d)\n",
             dest_row + x, dest_col + y,
             dest_row + x + block_width, dest_col + y + block_height);
      */
      /* copy from temp to the output file */

      copy2d(&out, dest_row + x, dest_col + y,
             &temp, 0, 0,
             block_width, block_height);


    }
  }
  

  free(temp.data);
}


void transposeTile(Array2d *dest, int dest_row, int dest_col,
                   Array2d *src, int src_row, int src_col,
                   int height, int width) {

  int x, y;
  static u64 bytes_done = 0, next_report = 100*1000*1000;

  /*
  printf("transpose (%d,%d) - (%d,%d)\n", src_row, src_col,
         src_row + height, src_col + width);
  */
  
  for (x = 0; x < width; x++) {
    for (y = 0; y < height; y++) {

      *Array2d_ptr(*dest, dest_row + x, dest_col + y) =
        *Array2d_ptr(*src, src_row + y, src_col + x);

    }

    /*
    if (block_bottom == out.n_cols) {
      writeNewline(Array2d_ptr(out, x, out.n_cols), newline_type);
    }
    */
  }

  bytes_done += (u64)width * height;
  if (bytes_done > next_report) {
    char buf1[50], buf2[50];
    printf("\r%s of %s bytes done, r=%.3f tx=%.3f w=%.3f",
           commafy(buf1, bytes_done), 
           commafy(buf2, (u64)in_file.n_rows * in_file.n_cols),
           time_reading, time_transposing, time_writing);
    fflush(stdout);
    next_report += 100*1000*1000;
  }
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

      if (src_row + height == in.n_rows)
        writeTileNewlines(src_col, src_col + width);

    }
  }

}


void transposeCacheOblivious(Array2d *dest, int dest_row, int dest_col,
                             Array2d *src, int src_row, int src_col,
                             int height, int width) {

  if (height > cache_ob_size || width > cache_ob_size) {
    int half;
    if (height > width) {
      half = height / 2;
      transposeCacheOblivious(dest, dest_row, dest_col,
                              src, src_row, src_col,
                              half, width);
      transposeCacheOblivious(dest, dest_row, dest_col+half,
                              src, src_row+half, src_col,
                              height-half, width);
    } else {
      half = width / 2;
      transposeCacheOblivious(dest, dest_row, dest_col,
                              src, src_row, src_col,
                              height, half);
      transposeCacheOblivious(dest, dest_row+half, dest_col,
                              src, src_row, src_col+half,
                              height, width-half);
    }
    return;
  }      

  transposeTile(dest, dest_row, dest_col,
                src, src_row, src_col, height, width);

  if (src_row + height == in.n_rows)
    writeTileNewlines(src_col, src_col + width);

}

                
/* simple copy without transposing */
void copy2d
(Array2d *dest, int dest_row, int dest_col,
 Array2d *src, int src_row, int src_col,
 int height, int width) {

  int row;

  for (row = 0; row < height; row++) {
    memcpy(Array2d_ptr(*dest, dest_row + row, dest_col),
           Array2d_ptr(*src, src_row + row, src_col),
           width);

    if (dest == &out && dest_col + width == dest->n_cols)
      writeNewline(Array2d_ptr(*dest, dest_row + row, dest->n_cols),
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
    if (width != pwrite(dest->fd, Array2d_ptr(*src, src_row + row, src_col),
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
    if (width != pread(src->fd, Array2d_ptr(*dest, dest_row + row, dest_col),
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
    length = f->n_rows * f->row_stride;
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

