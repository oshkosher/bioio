#ifndef __ZLINE_API_H__
#define __ZLINE_API_H__

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>

typedef struct ZlineIndexBlock{
  /* Offset, from the beginning of the file, where the compressed form
     of this block can be found. */
  uint64_t offset;

  /* Length of the compressed form of this block. */
  uint64_t compressed_length;
  
  /* Length of this block when decompressed. */
  uint64_t decompressed_length;
} ZlineIndexBlock;


typedef struct ZlineIndexLine {
  /* index of the block containing this line */
  uint64_t block_idx;

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

  /* buffer holding incoming data before compressing it */
  char *inbuf;
  int inbuf_size, inbuf_capacity;

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
  uint64_t blocks_capacity;  /* allocated size of blocks array */

  /* array of lines */
  ZlineIndexLine *lines;
  uint64_t lines_capacity;   /* allocated size of the lines array */
} ZlineFile;

#ifdef __cplusplus
extern "C" {
#endif

/* Create a new zlines file. */
ZlineFile *ZlineFile_create(const char *filename, int block_size);

/* Open an existing zlines file for reading. */
ZlineFile *ZlineFile_read(const char *filename);

/* length is the length of the line, and is optional. If 0, it will be
   computed using strlen. */
int ZlineFile_add_line(ZlineFile *zf, const char *line, uint64_t length);

/* Returns the number of lines in the file. */
uint64_t ZlineFile_line_count(ZlineFile *zf);

/* Returns the length of the given line. */
uint64_t ZlineFile_line_length(ZlineFile *zf, uint64_t line_idx);

/* Reads a line from the file. If buf is NULL, memory to store the line will
   be allocated with malloc. The caller must deallocate the memory with free().
   If buf is not NULL, the line will be written to that location.
   The caller can use ZlineFile_line_length() to check that their buffer
   is large enough.

   Return value: The location where the line was written, or NULL on error.

   If line_idx is >= ZlineFile_line_count(zf), or a memory allocation failed,
   or an error was encountered reading the file, this will return NULL.
*/
char *ZlineFile_get_line(ZlineFile *zf, uint64_t line_idx, char *buf);

void ZlineFile_close(ZlineFile *zf);

 
#ifdef __cplusplus
}
#endif



#endif /* __ZLINE_API_H__ */
