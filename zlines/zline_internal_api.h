/*
  zlines

  A tool for storing a large number of text lines in a compressed file
  and an API for accessing those lines efficiently.


  https://github.com/oshkosher/bioio/tree/master/zlines

  Ed Karrels, ed.karrels@gmail.com, January 2017
*/

#ifndef __ZLINE_INTERNAL_API_H__
#define __ZLINE_INTERNAL_API_H__

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include "zstd.h"
#include "zline_api.h"

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
} ZF;


#endif /* __ZLINE_INTERNAL_API_H__ */
