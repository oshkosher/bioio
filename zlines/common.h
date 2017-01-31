#ifndef __COMMON_H__
#define __COMMON_H__

#include <stdio.h>
#include <stdint.h>

#define MIN(a,b) ((a)<(b)?(a):(b))
#define MAX(a,b) ((a)>(b)?(a):(b))

/* If filename is "-", return stdin. Otherwise, open the given file
   for reading. */
FILE *openFileOrStdin(const char *filename);

/* Returns nonzero if the given file or directory exists */
int fileExists(const char *filename);

uint64_t getFileSize(const char *filename);

/* Return nonzero if the given filename exists and is a directory. */
int isDirectory(const char *filename);

/* If for_writing is nonzero, the file will be set to the length *length.
   Otherwise, *length will be set to the length of the file. */
int mapFile(const char *filename, int for_writing, char **data,
            uint64_t *length);


/* Formats a number with commas every 3 digits: 1234567 -> "1,234,567".
   The string will be written to buf (which must be at least 21 bytes),
   and buf will be returned.
*/
const char *commafy(char *buf, uint64_t n);

/* Return a relative time in seconds. */
double getSeconds();

/* Returns the amount of physical memory. */
uint64_t getMemorySize();
    
/* Parse a number with a case-insensitive magnitude suffix:
     k : multiply by 1024
     m : multiply by 1024*1024
     g : multiply by 1024*1024*1024

   For example, "32m" would parse as 33554432.
   Return nonzero on error.
*/
int parseSize(const char *str, uint64_t *result);


typedef struct {
  char *data;
  int n_rows, n_cols, row_stride;
} Array2d;

/* Get a pointer to a character in an Array2d. 'a' should be a Array2d*  */
#define Array2d_ptr(a, row, col) ((a)->data + (u64)(row) * (a)->row_stride + (col))

int Array2d_init(Array2d *array, int n_rows, int n_cols, int row_stride);


/* Transpose a 2d array using a cache-oblivious algorithm. */
void transpose
(Array2d *dest, int dest_row, int dest_col,
 Array2d *src, int src_row, int src_col,
 int height, int width);

/* Transpose one tile using a simple algorithm. */
void transposeTile
(Array2d *dest, int dest_row, int dest_col,
 Array2d *src, int src_row, int src_col,
 int height, int width);


#endif /* __COMMON_H__ */
