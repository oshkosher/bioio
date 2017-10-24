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
#include "zstd.h"


/* This the entry for one block of data when the block isn't necessarily
   in memory. */
typedef struct ZlineIndexBlock {
  /* Offset, from the beginning of the file, where the compressed form
     of this block can be found.
  */
  uint64_t offset;

  /* Length of the compressed content of this block.
     This does not include the length of the line index.
     Also, the most significant bit will be set iff the line index
     for this block is compressed.
  */
  uint64_t compressed_length_x;
  
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


/* This represents a block of data that is in memory. */
typedef struct ZlineBlock {
  /* index of this block */
  int64_t idx;
  
  /* Offset, from the beginning of the file, where the compressed form
     of this block can be found.
  */
  uint64_t offset;

  /* Index of the first line that starts in this block,
     which is lines[0]. */
  uint64_t first_line;

  /* array of lines stored in this block */
  ZlineIndexLine *lines;
  int lines_size, lines_capacity;

  /* contents of the lines stored in this block */
  char *content;
  int64_t content_size, content_capacity;

  /* Number of bytes used to encode the line index, which may be compressed
     or not. The compressed content is stored at offset + line_index_size.
     Set by loadLine().
  */
  uint64_t line_index_size;

} ZlineBlock;


/*
  file overhead = header + pad + blocks + lines
    header = 256
    pad = 0..7
    blocks = block_count * 24
    lines = line_count * 24
*/


typedef struct {
  char *filename;
  FILE *fp;
  int mode;

  /* Compressing the index will save space, but if the index is not
     compressed, it would be easier to memory-map the file and access
     data quickly. */
  int is_index_compressed;

  /* Current block being written. Use a pointer rather than including
     the struct inline because in the future the blocks may be compressed
     and written in a background thread. With a pointer it will be
     easier to pass a block to another thread. */
  ZlineBlock *write_block;

  /* Current block being read */
  ZlineBlock *read_block;

  /* Total number of lines in the file */
  uint64_t line_count;
  
  /* where the compressed line data starts in the file */
  uint64_t data_offset;

  /* where block index data starts in the file */
  uint64_t index_offset;

  /* Array of blocks.  When writing, the current block is
     blocks[block_count-1], and only the "offset" field is accurate. */
  ZlineIndexBlock *blocks;
  uint64_t blocks_size, blocks_capacity;
  
  /* Index of the first line in each block. This contains block_count-1
     entries, because the first block always starts with line 0.
     This is a cache of the data in ZlineBlock.first_line.
     This is sorted, so either a linear search can be used.
  */
  uint64_t *block_starts;

  uint64_t max_line_len;

  /* This is set to 1 whenever fseek is called. 
     When writing the file, the current file offset is always
     write_block->offset.
     If a line is read from and earlier block, fseek will need to be called
     to read the earlier block. If this flag has been set, then the caller
     will know it needs to restore the file pointer. */
  int fseek_flag;

  ZSTD_CStream *compress_stream;
  ZSTD_DStream *decompress_stream;
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

/* Create a new zlines file. block_size can be 2^32-1 at most.
   An int64_t type is used to avoid large value being silently truncated.
   If block_size is <= 0, 4 MB is used by default.
*/
ZLINE_EXPORT ZlineFile *ZlineFile_create
(const char *filename, int64_t block_size);
 

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

/* Returns the compressed or decompressed size of the given block.
   If block_idx is invalid, returns 0. */
ZLINE_EXPORT int ZlineFile_get_block_size_original
  (ZlineFile *zf, uint64_t block_idx);
ZLINE_EXPORT int ZlineFile_get_block_size_compressed
  (ZlineFile *zf, uint64_t block_idx);
  

ZLINE_EXPORT void ZlineFile_close(ZlineFile *zf);

 
#ifdef __cplusplus
}
#endif



#endif /* __ZLINE_API_H__ */
