/*
  zlines

  A tool for storing a large number of text lines in a compressed file
  and an API for accessing those lines efficiently.


  https://github.com/oshkosher/bioio/tree/master/zlines

  Ed Karrels, ed.karrels@gmail.com, January 2017
*/

#ifndef __ZLINE_API_H__
#define __ZLINE_API_H__

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>


/* One block of lines */
typedef struct ZlineIndexBlock {
  /* Offset, from the beginning of the file, where the compressed form
     of this block can be found. */
  uint64_t offset;

  /* Length of the compressed form of this block. */
  uint64_t compressed_length;
  
  /* Length of this block when decompressed. */
  uint64_t decompressed_length;
} ZlineIndexBlock;


/* On disk and in memory, there will be an array of line numbers, one
   per block, for quickly looking up the block in which a given line
   is stored.
*/


/* One line of data. */
typedef struct ZlineIndexLine {
  /* offset, in the decompressed block, where this line can be found. */
  uint64_t offset;

  /* length of this line (decompressed) */
  uint64_t length;
} ZlineIndexLine;

/*
  file overhead = header + pad + blocks + lines
    header = 256
    pad = 0..7
    blocks = block_count * 24
    lines = line_count * 24
*/


typedef struct {
  const char *filename;
  FILE *fp;
  int mode;
  int is_index_compressed;

  /* buffer holding incoming data before compressing it */
  char *inbuf;
  int inbuf_size, inbuf_capacity;

  // number of lines in inbuf
  int inbuf_count;

  /* buffer holding compressed data before writing it to the file */
  char *outbuf;
  int outbuf_size, outbuf_capacity;

  /* when reading the file, if outbuf_idx is not UINT64_MAX, then it
     is the index of the block whose decompressed data is currently
     in outbuf. This is used as a cache of the most recent decompressed
     block. */
  uint64_t outbuf_idx;

  uint64_t block_count, line_count;
  uint64_t data_offset, index_offset;

  /* array of blocks */
  ZlineIndexBlock *blocks;
  
  /* Index of the first line in each block. This contains block_count-1
     entries, because the first block always starts with line 0.
     This is sorted, so either a linear search can be used.
  */
  uint64_t *blocks_first_line;

  /* Allocated size of blocks array, and one more than the allocated size
     of the block_first_line array (reallocate them together). */  
  uint64_t blocks_capacity;

  /* array of lines in the current block */
  ZlineIndexLine *lines;
  uint64_t lines_capacity;   /* allocated size of the lines array */
} ZlineFile;


#ifdef __CYGWIN__
#define ZLINE_EXPORT __attribute__ ((visibility ("default")))
#elif defined(_WIN32)
#define ZLINE_EXPORT __declspec(dllexport)
#else
#define ZLINE_EXPORT
#endif

#ifdef __cplusplus
extern "C" {
#endif

/* Create a new zlines file. */
ZLINE_EXPORT ZlineFile *ZlineFile_create(const char *filename, int block_size);

/* Open an existing zlines file for reading. */
ZLINE_EXPORT ZlineFile *ZlineFile_read(const char *filename);

/* length is the length of the line, and is optional. If less than 0,
   it will be computed using strlen.

   Returns -1 if the file is opened for reading, or 0 on success.
*/
ZLINE_EXPORT int ZlineFile_add_line(ZlineFile *zf, const char *line, int64_t length);

ZLINE_EXPORT uint64_t ZlineFile_line_count(ZlineFile *zf);

/* Returns the length of the given line, or -1 if there is no such line. */
ZLINE_EXPORT int64_t ZlineFile_line_length(ZlineFile *zf, uint64_t line_idx);

/* Returns the length of the longest line. */
ZLINE_EXPORT uint64_t ZlineFile_max_line_length(ZlineFile *zf);

/* Reads a line from the file. If buf is NULL, memory to store the line will
   be allocated with malloc. The caller must deallocate the memory with free().
   If buf is not NULL, the line will be written to that location.
   The caller can use ZlineFile_line_length() to check that their buffer
   is large enough.

   Return value: The location where the line was written, or NULL on error.

   If line_idx is >= ZlineFile_line_count(zf), or a memory allocation failed,
   or an error was encountered reading the file, this will return NULL.
*/
ZLINE_EXPORT char *ZlineFile_get_line(ZlineFile *zf, uint64_t line_idx, char *buf);

/* Returns the number of compressed blocks in the file.
   If the file is open in write mode, this may under-report the block
   count by one. */
ZLINE_EXPORT uint64_t ZlineFile_get_block_count(ZlineFile *zf);

/* Fetches the compressed and decompressed size of the given block.
   If block_idx is invalid, returns -1.
   Returns 0 on success. */
ZLINE_EXPORT int ZlineFile_get_block_size(ZlineFile *zf, uint64_t block_idx,
                                          uint64_t *compressed_length,
                                          uint64_t *decompressed_length);

ZLINE_EXPORT void ZlineFile_close(ZlineFile *zf);

 
#ifdef __cplusplus
}
#endif



#endif /* __ZLINE_API_H__ */
