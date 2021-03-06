/*
  zlines

  A tool for storing a large number of text lines in a compressed file
  and an API for accessing those lines efficiently.


  https://github.com/oshkosher/bioio/tree/master/zlines

  Ed Karrels, ed.karrels@gmail.com, January 2017
*/

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <limits.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>

#ifndef _WIN32
#include <unistd.h>
#endif

#include "zline_api.h"
#include "zline_internal_api.h"
#include "zstd.h"
#include "common.h"

#define DEFAULT_BLOCK_SIZE (4*1024*1024)
#define MAX_IN_MEMORY_BLOCK DEFAULT_BLOCK_SIZE
#define INITIAL_BLOCK_CAPACITY 100
#define INITIAL_LINE_CAPACITY 100
#define HEADER_SIZE 256
#define ZSTD_COMPRESSION_LEVEL 3
#define MAX_HEADER_LINE_LEN 100
#define DO_COMPRESS_INDEX 1
#define FILE_BUFFER_SIZE 8192

#define ZLINE_MODE_CREATE 1
#define ZLINE_MODE_READ 2

typedef uint64_t u64;
typedef int64_t i64;

static int writeHeader(ZlineFile zf);
static int readHeader(ZlineFile zf);

/* Allocate a new ZlineBlock struct */
static ZlineBlock *createBlock(int content_capacity, int line_capacity);

/* Deallocate a ZlineBlock */
static void freeBlock(ZlineBlock *);

static void addLineInternal(ZlineFile zf, u64 length);
static void blocksInsureCapacity(ZlineFile zf, u64 capacity);
static void linesInsureCapacity(ZlineBlock *b, int capacity);
static void contentInsureCapacity(ZlineBlock *b, int capacity);
static void ZlineFile_deallocate(ZlineFile zf);

/* used by compressToFile to flush a buffer to disk */
static int flushOutbuf(ZlineFile zf, ZSTD_outBuffer *outbuf,
                       int64_t *bytes_written);

/* Returns the number of compressed bytes written to zf->fp or -1 on error */
static int64_t compressToFile(ZlineFile zf, const void *buf, int64_t len);

/* Returns # of bytes written to buf.
   readbuf_len - size of readbuf. Write no more than this many bytes.
   compressed_len - size of the compressed data on disk.
   read_offset - skip this many (decompressed) bytes at the beginning
*/
static int64_t decompressFromFile
  (ZlineFile zf, void *readbuf, int64_t readbuf_len,
   int64_t compressed_len, uint64_t read_offset);

/* Flush the current write_block. Return a pointer to the new write_block
   (which may be the same one). */
static ZlineBlock* flushBlock(ZlineFile zf);

/* Given the line index for one block, see if compressing the index makes
   it smaller. If so, return the compressed data and its size in *data
   and *len and return true. Otherwise return false. */
static int useCompressedLineIndex(ZlineFile zf, void **data, uint64_t *len);

/* Handle the hack where ZlineIndexBlock.compressed_length_x contains both
   the compressed length of a block and a bit noting whether the line
   index for that block is compressed. */
#define LINE_INDEX_COMPRESSED_FLAG ((u64)1 << 63)
#define getBlockCompressedLen(block) \
  ((block)->compressed_length_x & ~LINE_INDEX_COMPRESSED_FLAG)
#define isBlockLineIndexCompressed(block) \
  ((((block)->compressed_length_x) >> 63) & 1)

/* returns the index of the block containing this line */
static u64 getLineBlock(ZlineFile zf, u64 line_idx);

/* Make sure a line is in memory.
   Set *block to the either zf->write_block or zf->read_block, whichever
   contains the line.
   Set *line to point to the record of its offset and length.
   If the line isn't in memory, the block containing it will
   be loaded into zf->read_block.
*/
static int loadLine(ZlineFile zf, u64 line_idx, ZlineIndexLine **line,
                    ZlineBlock **block);

/* If the given line starts in this block, return a pointer to its index entry.
   Otherwise return NULL. */
static ZlineIndexLine* lineInBlock(ZlineBlock *block, u64 line_idx);

/* read a block, store the decompressed result in read_block */
static int readBlock(ZlineFile zf, u64 block_idx);


/* Creates a new zlines file using blocks of the default size
   (currently 4 MiB).

   Use the result as the 'zf' argument to other functions in this
   module.

   Call ZlineFile_close(zf) to finish writing the file and close it.
*/
ZLINE_EXPORT ZlineFile ZlineFile_create(const char *output_filename) {
  return ZlineFile_create2(output_filename, DEFAULT_BLOCK_SIZE);
}

  
/* Like ZlineFile_create, but the user can select the block size. */
ZLINE_EXPORT ZlineFile ZlineFile_create2(const char *output_filename,
                                         uint64_t block_size) {

  if (block_size > INT_MAX) {
    fprintf(stderr, "ZlineFile_create error: block_size too large\n");
    return NULL;
  } else if (block_size == 0) {
    block_size = DEFAULT_BLOCK_SIZE;
  }
  
  ZlineFile zf = (ZlineFile) calloc(1, sizeof(struct ZlineFile));
  if (!zf) {
    fprintf(stderr, "Out of memory\n");
    return NULL;
  }

  zf->filename = strdup(output_filename);
  zf->mode = ZLINE_MODE_CREATE;
  zf->is_index_compressed = DO_COMPRESS_INDEX;
  zf->data_offset = HEADER_SIZE;
  zf->write_block = createBlock(block_size, -1);
  zf->write_block->idx = 0;

  /* open the output file */
  zf->fp = fopen(output_filename, "w+b");
  if (!zf->fp) goto fail;

  if (writeHeader(zf)) goto fail;

  blocksInsureCapacity(zf, INITIAL_BLOCK_CAPACITY);
  linesInsureCapacity(zf->write_block, INITIAL_LINE_CAPACITY);

  zf->blocks_size = 1;
  zf->blocks[0].offset = 0;
  zf->blocks[0].decompressed_length = 0;
  zf->blocks[0].compressed_length_x = 0;

  zf->write_block->offset = HEADER_SIZE;

  zf->compress_stream = ZSTD_createCStream();
  assert(zf->compress_stream);

  /* only allocate if needed */
  zf->decompress_stream = NULL;
  
  return zf;

 fail:
  ZlineFile_deallocate(zf);

  return NULL;
}


static int writeHeader(ZlineFile zf) {
  char buf[HEADER_SIZE];
  int pos = 0;
  size_t write_len;

  pos += sprintf(buf, "zline v2.0\n");
  pos += sprintf(buf+pos, "data_offset %" PRIu64 "\n", zf->data_offset);
  pos += sprintf(buf+pos, "index_offset %" PRIu64 "\n", zf->index_offset);
  pos += sprintf(buf+pos, "lines %" PRIu64 "\n", zf->line_count);
  pos += sprintf(buf+pos, "blocks %" PRIu64 "\n", zf->blocks_size);
  pos += sprintf(buf+pos, "maxlen %" PRIu64 "\n", zf->max_line_len);
  pos += sprintf(buf+pos, "alg fzstd\n");
  if (zf->is_index_compressed)
    pos += sprintf(buf+pos, "zi\n");
  buf[pos++] = '\n';
  assert(pos <= HEADER_SIZE);

  /* fill the header with whitespace */
  memset(buf+pos, ' ', HEADER_SIZE - 1 - pos);
  buf[HEADER_SIZE-1] = '\n';
  
  if (fseek(zf->fp, 0, SEEK_SET)) {
    fprintf(stderr, "Failed to write header for \"%s\"\n", zf->filename);
    return -1;
  }

  write_len = fwrite(buf, 1, HEADER_SIZE, zf->fp);
  
  if (write_len != HEADER_SIZE) {
    fprintf(stderr, "Failed to write header for \"%s\"\n", zf->filename);
    return -1;
  }
  fflush(zf->fp);

  assert((u64)ftell(zf->fp) == zf->data_offset);
  
  return 0;
}


static int readHeader(ZlineFile zf) {
  char buf[MAX_HEADER_LINE_LEN], word[MAX_HEADER_LINE_LEN];

  assert(zf->fp);

  zf->is_index_compressed = 0;
  
  if (fseek(zf->fp, 0, SEEK_SET)) {
    fprintf(stderr, "Failed to move to top of file to read header.\n");
    return 1;
  }

  if (!fgets(buf, MAX_HEADER_LINE_LEN, zf->fp))
    goto format_error;
  
  if (strncmp(buf, "zline v2.0", 10))
    goto format_error;

  while (fgets(buf, MAX_HEADER_LINE_LEN, zf->fp) && buf[0] != '\n') {
    int pos;
    sscanf(buf, "%s %n", word, &pos);

    if (!strcmp(word, "data_offset")) {
      if (1 != sscanf(buf+pos, "%" SCNu64, &zf->data_offset)) goto format_error;
    } else if (!strcmp(word, "index_offset")) {
      if (1 != sscanf(buf+pos, "%" SCNu64, &zf->index_offset)) goto format_error;
    } else if (!strcmp(word, "lines")) {
      if (1 != sscanf(buf+pos, "%" SCNu64, &zf->line_count)) goto format_error;
    } else if (!strcmp(word, "blocks")) {
      if (1 != sscanf(buf+pos, "%" SCNu64, &zf->blocks_size)) goto format_error;
    } else if (!strcmp(word, "maxlen")) {
      if (1 != sscanf(buf+pos, "%" SCNu64, &zf->max_line_len)) goto format_error;
    } else if (!strcmp(word, "alg")) {
      if (1 != sscanf(buf+pos, "%s", word)) goto format_error;
      if (strcmp(word, "fzstd")) {
        fprintf(stderr, "Unrecognized compression algorithm: \"%s\"\n", word);
        return 1;
      }
    } else if (!strcmp(word, "zi")) {
      zf->is_index_compressed = 1;
    } else {
      goto format_error;
    }
  }

  if (zf->data_offset == 0 ||
      zf->index_offset == 0 ||
      zf->line_count == 0 ||
      zf->blocks_size == 0) {
    fprintf(stderr, "File header incomplete\n");
    return 1;
  }
  
  return 0;

 format_error:
  fprintf(stderr, "Error reading \"%s\", invalid format\n", zf->filename);
  return 1;
}


/* Allocate a new ZlineBlock struct */
static ZlineBlock *createBlock(int content_capacity, int line_capacity) {
  ZlineBlock *b = (ZlineBlock*) calloc(1, sizeof(ZlineBlock));
  assert(b);

  if (content_capacity <= 0)
    content_capacity = INITIAL_BLOCK_CAPACITY;
  if (line_capacity <= 0)
    line_capacity = INITIAL_LINE_CAPACITY;

  linesInsureCapacity(b, line_capacity);
  contentInsureCapacity(b, content_capacity);

  return b;
}


/* Deallocate a ZlineBlock */
static void freeBlock(ZlineBlock *b) {
  if (!b) return;

  free(b->lines);
  free(b->content);
  free(b);
}


static void addLineInternal(ZlineFile zf, u64 length) {
  ZlineIndexLine *line;
  ZlineBlock *b = zf->write_block;
  assert(b);
  
  linesInsureCapacity(b, b->lines_size + 1);
  line = b->lines + b->lines_size;
  line->offset = b->content_size;
  line->length = length;
  if (b->lines_size == 0) {
    b->first_line = zf->line_count;
    if (b->idx > 0) zf->block_starts[b->idx-1] = zf->line_count;
  }

  /*
  printf("line %d: block %d, offset %d, length %d\n",
         (int)zf->line_count, (int)block_idx, (int)line->offset,
         (int)line->length);
  */

  b->lines_size++;
  zf->line_count++;
  zf->max_line_len = MAX(length, zf->max_line_len);
}


/* Given the line index for one block, see if compressing the index makes
   it smaller. If so, return the compressed data and its size in *data
   and *len and return true. Otherwise return false. */
static int useCompressedLineIndex
(ZlineFile zf, void **compressed_line_index, uint64_t *compressed_len) {

  ZlineBlock *b = zf->write_block;
  size_t result, array_size, buf_size;
  char *buf;

  assert(zf->compress_stream);

  /* never compress blocks with less than two lines in them */
  if (b->lines_size < 2) return 0;

  array_size = b->lines_size * sizeof(ZlineIndexLine);
  buf_size = ZSTD_compressBound(array_size);
  buf = (char*) malloc(buf_size);
  assert(buf);

  /*
  result = ZSTD_compressCCtx(zf->compress_stream, buf, buf_size,
                             b->lines, array_size, ZSTD_COMPRESSION_LEVEL);
  */
  result = ZSTD_compress(buf, buf_size, b->lines, array_size,
                         ZSTD_COMPRESSION_LEVEL);
  
  assert(!ZSTD_isError(result));

  if (result + sizeof(uint64_t) < array_size) {
    *compressed_line_index = buf;
    *compressed_len = result;
    return 1;
  } else {
    free(buf);
    return 0;
  }
}


/* Flush the current write_block. Return a pointer to the new write_block
   (which may be the same one).
*/
static ZlineBlock* flushBlock(ZlineFile zf) {
  ZlineIndexBlock *block_idx = zf->blocks + (zf->blocks_size-1);
  ZlineBlock *b = zf->write_block;
  int64_t write_len, compressed_len, line_index_len;
  u64 next_block_start, block_no;
  void *compressed_line_index;
  uint64_t compressed_line_index_len;
  uint64_t compressed_line_index_flag = 0;

  assert(b);
  assert(b->offset > 0);

  /* if there is no data in the block, do nothing */
  if (b->content_size == 0) return b;

  block_idx->offset = b->offset;
  block_idx->decompressed_length = b->content_size;
  if (b->idx > 0)
    zf->block_starts[b->idx - 1] = b->first_line;

  /* XXX expensive assert */
  assert((u64)ftell(zf->fp) == b->offset);

  if (b->lines_size == 0) {
    line_index_len = 0;
  } else {

    /* Try compressing the line index. If it's smaller, leave the data
       and its size in compressed_line_index and compressed_line_index_len,
       respectively and return true. Otherwise, return false. */
    if (useCompressedLineIndex(zf, &compressed_line_index,
                               &compressed_line_index_len)) {
      /* write 4-byte length of compressed line index */
      write_len = fwrite(&compressed_line_index_len,
                         sizeof compressed_line_index_len, 1, zf->fp);
      if (write_len != 1) goto fail;

      /* write compressed line index */
      write_len = fwrite(compressed_line_index, 1, compressed_line_index_len,
                         zf->fp);
      if (write_len != compressed_line_index_len) goto fail;
      free(compressed_line_index);
    
      line_index_len = (sizeof compressed_line_index_len) +
        compressed_line_index_len;
      compressed_line_index_flag = LINE_INDEX_COMPRESSED_FLAG;

    } else {
      /* write uncompressed line index */
      write_len = fwrite(b->lines, sizeof(ZlineIndexLine), b->lines_size,
                         zf->fp);
      if (write_len != b->lines_size) goto fail;
      line_index_len = sizeof(ZlineIndexLine) * b->lines_size;
    }
    
  }
  
  /* compress the line contents */
  compressed_len = compressToFile(zf, b->content, b->content_size);
  if (compressed_len < 0) return b;
  
  assert(compressed_len > 0);
  next_block_start = b->offset + line_index_len + compressed_len;
  block_idx->compressed_length_x = compressed_len | compressed_line_index_flag;

  /* check we're at the expected offset in the file */
  /* XXX slow assert */
  assert(ftell(zf->fp) == next_block_start);

  /* move to the next block and initialize its offset */
  /* XXX why +2? */
  blocksInsureCapacity(zf, zf->blocks_size + 2);
  block_no = zf->blocks_size;
  zf->blocks_size++;
  zf->blocks[block_no].offset = b->offset = next_block_start;
  zf->blocks[block_no].decompressed_length = 0;
  zf->blocks[block_no].compressed_length_x = 0;
  zf->block_starts[block_no-1] = zf->line_count;

  b->idx = block_no;
  b->lines_size = 0;
  b->content_size = 0;

  return b;

 fail:
  fprintf(stderr, "Error writing compressed block.\n");
  return NULL;
}
            
  
/* Adds a line of text to the file.
   Returns -1 if the file is opened for reading, or 0 on success.
*/
ZLINE_EXPORT int ZlineFile_add_line(ZlineFile zf, const char *line) {
  return ZlineFile_add_line2(zf, line, strlen(line));
}


/* Like ZlineFile_add_line, but the length of the line is supplied. */
ZLINE_EXPORT int ZlineFile_add_line2(ZlineFile zf, const char *line,
                                     uint64_t length) {
  ZlineBlock *b;

  if (zf->mode != ZLINE_MODE_CREATE) {
    return -1;
  }
  
  if (length < 0) length = strlen(line);
  
  b = zf->write_block;
  assert(b);

  /* the line doesn't fit in the current block, flush the current block */
  if (b->content_size + length > (u64)b->content_capacity) {
    b = flushBlock(zf);
  }

  /* add an entry to the line index */
  addLineInternal(zf, length);

  if (length <= (u64)b->content_capacity) {
    /* add the line to the content buffer */
    memcpy(b->content + b->content_size, line, length);
    b->content_size += length;
  } else {
    /* for really long lines, send them straight to disk */
    char *content_buffer_saved = b->content;
    assert(b->content_size == 0);
    b->content = (char*) line;
    b->content_size = length;
    flushBlock(zf);
    b->content = content_buffer_saved;
  }    

  return 0;
}

  
/* If the file is open for writing, this finishes writing the file.
   The file is closed, and any memory allocated internally is deallocated. */
ZLINE_EXPORT void ZlineFile_close(ZlineFile zf) {
  size_t write_len;
  int pad_size;
  char pad_buf[7] = {0};
  u64 current_pos;

  if (zf->mode == ZLINE_MODE_READ) goto ok;

  assert(zf->write_block);
  assert(zf->write_block->idx == zf->blocks_size-1);
  
  if (zf->write_block->content_size > 0) {
    flushBlock(zf);
  }
  zf->blocks_size--;
    
  current_pos = zf->write_block->offset;
  
  /* XXX slow assert */
  assert((u64)ftell(zf->fp) == current_pos);

  /* pad the file so the index is 8-byte aligned */
  pad_size = (~current_pos + 1) & 7;
  zf->index_offset = current_pos + pad_size;
  assert(pad_size >= 0 &&
         pad_size <= 7 &&
         zf->index_offset % 8 == 0);

  write_len = fwrite(pad_buf, 1, pad_size, zf->fp);
  if (write_len != (size_t)pad_size) goto fail;

  /* write the index */
  if (zf->is_index_compressed) {
    u64 size_block_array_compressed, size_starts_array_compressed;

    /* fill in the compressed sizes later */
    if (fseek(zf->fp, sizeof(u64) * 2, SEEK_CUR)) goto fail;

    /* write the block index */
    size_block_array_compressed = compressToFile
      (zf, zf->blocks, sizeof(ZlineIndexBlock) * zf->blocks_size);

    /* write the block_starts array */
    size_starts_array_compressed = compressToFile
      (zf, zf->block_starts, sizeof(u64) * (zf->blocks_size - 1));

    /* write the compressed sizes of the arrays */
    if (fseek(zf->fp, zf->index_offset, SEEK_SET)) goto fail;
    
    fwrite(&size_block_array_compressed, sizeof(u64), 1, zf->fp);
    fwrite(&size_starts_array_compressed, sizeof(u64), 1, zf->fp);
  }

  /* write uncompressed block index */
  else {
    if (zf->blocks_size != fwrite(zf->blocks, sizeof(ZlineIndexBlock),
                                  zf->blocks_size, zf->fp))
      goto fail;
    
    if (zf->blocks_size-1 != fwrite(zf->block_starts, sizeof(u64),
                                    zf->blocks_size - 1, zf->fp))
      goto fail;
  }

  /* write the header */
  if (writeHeader(zf)) goto fail;

  /* close the file */
    goto ok;
 fail:
    fprintf(stderr, "Error writing index to \"%s\"\n", zf->filename);
 ok:

  ZlineFile_deallocate(zf);
}


static void blocksInsureCapacity(ZlineFile zf, u64 capacity) {
  u64 new_cap;
  if (!zf->blocks || zf->blocks_capacity < capacity) {
    new_cap = zf->blocks_capacity * 2;
    if (new_cap < capacity) new_cap = capacity;
    zf->blocks = (ZlineIndexBlock*) realloc
      (zf->blocks, sizeof(ZlineIndexBlock) * new_cap);
    if (!zf->blocks) goto fail;
    if (new_cap > 1) {
      zf->block_starts = (uint64_t*) realloc
        (zf->block_starts, sizeof(uint64_t) * (new_cap - 1));
      if (!zf->block_starts) goto fail;
    }
    zf->blocks_capacity = new_cap;
  }

  return;
  
 fail:
  fprintf(stderr, "Out of memory allocating block array\n");
  assert(0);
}

  
static void linesInsureCapacity(ZlineBlock *b, int capacity) {
  int new_cap;
  if (!b->lines || b->lines_capacity < capacity) {
    new_cap = b->lines_capacity * 2;
    if (new_cap < capacity) new_cap = capacity;
    b->lines = (ZlineIndexLine*) realloc
      (b->lines, sizeof(ZlineIndexLine) * new_cap);
    if (!b->lines) {
      fprintf(stderr, "Out of memory allocating line array\n");
      assert(0);
    }
    b->lines_capacity = new_cap;
  }
}


static void contentInsureCapacity(ZlineBlock *b, int capacity) {
  int new_cap;
  if (!b->content || b->content_capacity < capacity) {
    new_cap = b->content_capacity * 2;
    if (new_cap < capacity) new_cap = capacity;
    b->content = (char*) realloc(b->content, new_cap);
    if (!b->content) {
      fprintf(stderr, "Out of memory allocating content array\n");
      assert(0);
    }
    b->content_capacity = new_cap;
  }
}


static void ZlineFile_deallocate(ZlineFile zf) {
  if (!zf) return;

  if (zf->compress_stream)
    ZSTD_freeCStream(zf->compress_stream);
  if (zf->decompress_stream)
    ZSTD_freeDStream(zf->decompress_stream);
  if (zf->fp) fclose(zf->fp);
  freeBlock(zf->write_block);
  freeBlock(zf->read_block);
  free(zf->blocks);
  free(zf->block_starts);
  free(zf->filename);
  free(zf);
}


static int flushOutbuf(ZlineFile zf, ZSTD_outBuffer *outbuf,
                       int64_t *bytes_written) {
  size_t write_len;

  write_len = fwrite(outbuf->dst, 1, outbuf->pos, zf->fp);
  if (write_len != outbuf->pos) {
    fprintf(stderr, "Failed to write to \"%s\"\n", zf->filename);
    return 1;
  } else {
    *bytes_written += write_len;
    outbuf->pos = 0;
    return 0;
  }
}


/* Return the number of compressed bytes written to zf->fp */
static int64_t compressToFile(ZlineFile zf, const void *input,
                              int64_t input_len) {
  unsigned char buf[FILE_BUFFER_SIZE];
  ZSTD_outBuffer outbuf;
  ZSTD_inBuffer inbuf;
  size_t write_len;
  int64_t bytes_written = 0;

  /* make sure the input data is not larger than a size_t can represent */
  assert(input_len <= SIZE_MAX);
  assert(zf->compress_stream);

  if (input_len == 0) return 0;

  inbuf.src = input;
  inbuf.size = input_len;
  inbuf.pos = 0;

  outbuf.dst = buf;
  outbuf.size = sizeof buf;
  outbuf.pos = 0;

  ZSTD_initCStream(zf->compress_stream, ZSTD_COMPRESSION_LEVEL);

  while (inbuf.pos < inbuf.size) {
    ZSTD_compressStream(zf->compress_stream, &outbuf, &inbuf);

    /* either the input data has been fully processed or
       the output buffer is full */
    assert(inbuf.pos == inbuf.size || outbuf.pos == outbuf.size);

    /* only flush the output buffer if it is full, because the last bit
       can probably be flushed along with the end-of-stream data. */
    if (outbuf.pos == outbuf.size)
      if (flushOutbuf(zf, &outbuf, &bytes_written)) return -1;
  }

  while (1) {
    write_len = ZSTD_endStream(zf->compress_stream, &outbuf);
    if (ZSTD_isError(write_len)) {
      fprintf(stderr, "Error writing compressed data\n");
      assert(write_len == 0);
    }
    if (write_len == 0) break;
    if (flushOutbuf(zf, &outbuf, &bytes_written)) return -1;
  }

  assert(write_len == 0);

  if (outbuf.pos > 0)
    flushOutbuf(zf, &outbuf, &bytes_written);
  
  return bytes_written;
}


/* return # of bytes written to readbuf */
static int64_t decompressFromFile
  (ZlineFile zf, void *readbuf, int64_t readbuf_len,
   int64_t compressed_len, uint64_t read_offset) {

  unsigned char buf[FILE_BUFFER_SIZE];
  ZSTD_outBuffer outbuf;
  ZSTD_inBuffer inbuf;
  size_t to_read, bytes_read, result;
  int64_t input_remaining = compressed_len, skip_remaining = read_offset;

  /* make sure the input data is not larger than a size_t can represent */
  assert(compressed_len <= SIZE_MAX);
  assert(zf->decompress_stream);

  if (compressed_len == 0) return 0;

  inbuf.src = buf;
  inbuf.size = 0;
  inbuf.pos = 0;
  
  ZSTD_initDStream(zf->decompress_stream);

  /* skip the first 'skip_remaining' bytes */
  while (input_remaining > 0 && skip_remaining > 0) {
    unsigned char junk_buf[FILE_BUFFER_SIZE];
    outbuf.dst = junk_buf;
    outbuf.size = MIN(FILE_BUFFER_SIZE, skip_remaining);
    outbuf.pos = 0;

    /* move unconsumed input data to the top of the buffer */
    if (inbuf.pos < inbuf.size) {
      memmove(buf, buf + inbuf.pos, inbuf.size - inbuf.pos);
    }              
    inbuf.size -= inbuf.pos;
    inbuf.pos = 0;

    /* fill the input buffer with compressed data */
    to_read = MIN(sizeof buf - inbuf.size, input_remaining);
    bytes_read = fread(buf + inbuf.size, 1, to_read, zf->fp);
    if (bytes_read != to_read) {
      fprintf(stderr, "Failed to read from \"%s\"\n", zf->filename);
      return 0;
    }
    inbuf.size += bytes_read;
    input_remaining -= bytes_read;

    /* decompress to junk buffer */
    result = ZSTD_decompressStream(zf->decompress_stream, &outbuf, &inbuf);
    if (ZSTD_isError(result)) {
      fprintf(stderr, "Error decompressing data from \"%s\"\n",
              zf->filename);
      return 0;
    }

    skip_remaining -= outbuf.pos;
  }

  outbuf.dst = readbuf;
  outbuf.size = readbuf_len;
  outbuf.pos = 0;

  /* Stop if all compressed data has been read or readbuf is full.
     If readbuf fills up before all the content has been decompressed,
     it's the caller's problem. */
  while (input_remaining > 0 && outbuf.pos < outbuf.size) {

    /* move unconsumed input data to the top of the buffer */
    if (inbuf.pos < inbuf.size) {
      memmove(buf, buf + inbuf.pos, inbuf.size - inbuf.pos);
    }              
    inbuf.size -= inbuf.pos;
    inbuf.pos = 0;

    /* fill the input buffer with compressed data */
    to_read = MIN(sizeof buf - inbuf.size, input_remaining);
    bytes_read = fread(buf + inbuf.size, 1, to_read, zf->fp);
    if (bytes_read != to_read) {
      fprintf(stderr, "Failed to read from \"%s\"\n", zf->filename);
      return 0;
    }
    inbuf.size += bytes_read;
    input_remaining -= bytes_read;

    /* decompress */
    result = ZSTD_decompressStream(zf->decompress_stream, &outbuf, &inbuf);
    if (ZSTD_isError(result)) {
      fprintf(stderr, "Error decompressing data from \"%s\"\n",
              zf->filename);
      return 0;
    }
  }

  return outbuf.pos;
}
  


/* Open an existing zlines file for reading.
   Use the result as the 'zf' argument to other functions in this module.
   Call ZlineFile_close(zf) to close the file.
*/
ZLINE_EXPORT ZlineFile ZlineFile_read(const char *filename) {
  ZlineFile zf = (ZlineFile) calloc(1, sizeof(struct ZlineFile));
  size_t read_len;
  u64 file_size, block_idx;
  int64_t bytes_read;
  int max_content_len = 0, max_line_count = 0;
  
  assert(zf);

  zf->filename = strdup(filename);
  zf->mode = ZLINE_MODE_READ;

  file_size = getFileSize(filename);
  
  zf->fp = fopen(filename, "rb");
  if (!zf->fp) goto fail;

  zf->decompress_stream = ZSTD_createDStream();

  /* read the header */
  if (readHeader(zf)) goto fail;

  /* allocate space for the index */
  blocksInsureCapacity(zf, zf->blocks_size);

  if (fseek(zf->fp, zf->index_offset, SEEK_SET)) goto fail;

  /* read the index */

  if (zf->is_index_compressed) {
    /* [0]: block array, [1]: line array */
    u64 compressed_sizes[2];

    if (2 != fread(compressed_sizes, sizeof(u64), 2, zf->fp)) goto fail;

    if (zf->index_offset + compressed_sizes[0] >= file_size ||
        zf->index_offset + compressed_sizes[1] >= file_size ||
        zf->index_offset + compressed_sizes[0] + compressed_sizes[1]
        + 2*sizeof(u64) != file_size) {
      fprintf(stderr, "Error in compressed index size\n");
      goto fail;
    }

    /* read the compressed block index */
    bytes_read = decompressFromFile
      (zf, zf->blocks, zf->blocks_size * sizeof(ZlineIndexBlock),
       compressed_sizes[0], 0);
    if (bytes_read != zf->blocks_size * sizeof(ZlineIndexBlock))
      goto fail;

    /* read the compressed line starts */
    bytes_read = decompressFromFile
      (zf, zf->block_starts, (zf->blocks_size-1) * sizeof(u64),
       compressed_sizes[1], 0);
    if (bytes_read != (zf->blocks_size-1) * sizeof(u64))
      goto fail;
  }

  /* read uncompressed index */
  else {
    read_len = fread(zf->blocks, sizeof(ZlineIndexBlock), zf->blocks_size,
                     zf->fp);
    if (read_len != zf->blocks_size) goto fail;
    
    read_len = fread(zf->block_starts, sizeof(u64), zf->blocks_size-1, zf->fp);
    if (read_len != zf->blocks_size - 1) goto fail;
  }

  /* scan the index to find the largest content */
  for (block_idx = 0; block_idx < zf->blocks_size; block_idx++) {
    max_content_len = MAX(max_content_len,
                          zf->blocks[block_idx].decompressed_length);
    max_line_count = MAX(max_line_count, ZlineFile_get_block_line_count(zf, block_idx));
  }

  zf->read_block = createBlock(max_content_len, max_line_count);
  if (!zf->read_block) {
    fprintf(stderr, "Out of memory opening file\n");
    goto fail;
  }
  zf->read_block->idx = -1;
  
  return zf;

 fail:
  ZlineFile_deallocate(zf);

  return NULL;
}


/* Returns the number of lines in the file. */
ZLINE_EXPORT uint64_t ZlineFile_line_count(ZlineFile zf) {
  return zf->line_count;
}


/* Returns the length of the given line or -1 if there is no such line. */
ZLINE_EXPORT int64_t ZlineFile_line_length(ZlineFile zf, uint64_t line_idx) {
  ZlineIndexLine *line;
  ZlineBlock *block;
  
  if (line_idx >= zf->line_count)
    return -1;

  loadLine(zf, line_idx, &line, &block);
  assert(line);
  
  return line->length;
}


/* Returns the length of the longest line. */
ZLINE_EXPORT uint64_t ZlineFile_max_line_length(ZlineFile zf) {
  return zf->max_line_len;
}


/* returns the index of the block containing this line */
static u64 getLineBlock(ZlineFile zf, u64 line_idx) {
  if (zf->blocks_size <= 1) {
    return 0;
  } else {
    u64 i;
    /* linear scan */
    /* XXX change to binary scan for larger arrays */
    for (i=0; i < zf->blocks_size-1; i++) {
      if (line_idx <= zf->block_starts[i] - 1) {
        return i;
      }
    }
    return i;
  }
}


/* Make sure a line is in memory.
   Set *block to the either zf->write_block or zf->read_block, whichever
   contains the line.
   Set *line to point to the record of its offset and length.
   If the line isn't in memory, the block containing it will
   be loaded into zf->read_block.
*/
static int loadLine(ZlineFile zf, u64 line_idx, ZlineIndexLine **line,
                    ZlineBlock **block) {
  u64 block_idx;

  /* check if the line is in the write block */
  if (zf->mode == ZLINE_MODE_CREATE) {
    *line = lineInBlock(zf->write_block, line_idx);
    if (*line) {
      *block = zf->write_block;
      return 0;
    }
  }

  /* check if it's in the read block */
  *line = lineInBlock(zf->read_block, line_idx);
  if (*line) {
    *block = zf->read_block;
    return 0;
  }

  /* load the block containing the line into the read block */
  block_idx = getLineBlock(zf, line_idx);
  readBlock(zf, block_idx);
  *line = lineInBlock(zf->read_block, line_idx);
  assert(*line);
  *block = zf->read_block;

  return 0;
}


ZLINE_EXPORT int ZlineFile_get_line_details
  (ZlineFile zf, uint64_t line_idx, uint64_t *length,
   uint64_t *offset, uint64_t *block_idx) {
  ZlineIndexLine *line;
  ZlineBlock *block;

  if (line_idx >= zf->line_count) return -1;
  
  *block_idx = getLineBlock(zf, line_idx);
  loadLine(zf, line_idx, &line, &block);
  *length = line->length;
  *offset = line->offset;
  return 0;
}


static ZlineIndexLine* lineInBlock(ZlineBlock *block, u64 line_idx) {
  if (block &&
      block->lines_size > 0 &&
      line_idx >= block->first_line &&
      line_idx < block->first_line + block->lines_size)
    return block->lines + (line_idx - block->first_line);
  else
    return NULL;
}


/* Read a block, store the decompressed result in zf->read_block.
   Return nonzero on error. 
   If the data is larger than a normal block, just read the line index.
*/
static int readBlock(ZlineFile zf, u64 block_idx) {
  ZlineIndexBlock *block;
  ZlineBlock *b;
  int block_line_count;
  int64_t line_bytes, bytes_read;

  assert(block_idx < zf->blocks_size);

  /* check if we already have this block decompressed */
  if (zf->read_block && block_idx == zf->read_block->idx) {
    return 0;
  }

  if (!zf->decompress_stream)
    zf->decompress_stream = ZSTD_createDStream();

  block = zf->blocks + block_idx;

  /* compute the number of lines in this block */
  block_line_count = ZlineFile_get_block_line_count(zf, block_idx);

  /* allocate zf->read_block if needed, otherwise make sure its
     content and line arrays are big enough */
  if (!zf->read_block) {
    zf->read_block = createBlock(block->decompressed_length, block_line_count);
  } else {
    linesInsureCapacity(zf->read_block, block_line_count);
  }

  b = zf->read_block;
  b->idx = block_idx;
  b->offset = block->offset;
  b->first_line = (block_idx == 0) ? 0 : zf->block_starts[block_idx-1];

  zf->fseek_flag = 1;
  if (fseek(zf->fp, block->offset, SEEK_SET)) {
    fprintf(stderr, "Failed to seek to block %" PRIu64 " offset %" PRIu64 "\n",
            block_idx, block->offset);
    return 1;
  }

  b->lines_size = block_line_count;
  line_bytes = b->lines_size * sizeof(ZlineIndexLine);

  /* Read line index */
  if (isBlockLineIndexCompressed(block)) {
    uint64_t compressed_index_len;
    if (1 != fread(&compressed_index_len, sizeof compressed_index_len, 1,
                   zf->fp))
      goto fail;

    b->line_index_size = compressed_index_len;

    bytes_read = decompressFromFile(zf, b->lines, line_bytes,
                                    compressed_index_len, 0);
  } else {
    b->line_index_size = line_bytes;
    bytes_read = fread(b->lines, 1, line_bytes, zf->fp);
  }
  if (bytes_read != line_bytes)
    goto fail;

  /* If the block is small enough, decompress it all into memory now.
     If it's large, don't load the content into memory. */
  if (block_line_count == 1 &&
      block->decompressed_length > MAX_IN_MEMORY_BLOCK) {
    b->content_size = 0;
  } else {
    contentInsureCapacity(zf->read_block, block->decompressed_length);

    /* Read compressed content */
    b->content_size = block->decompressed_length;
    bytes_read = decompressFromFile(zf, b->content, b->content_size,
                                    getBlockCompressedLen(block), 0);
    if (bytes_read != b->content_size)
      goto fail;
  }

  return 0;

fail:
  fprintf(stderr, "Failed to read block %" PRIu64 "\n", block_idx);
  return 1;
}


/* Reads a line from the file and returns it as a string.
   The result has been allocated with malloc(); the caller is responsible
   for deallocating it with free().

   If line_idx is invalid, a memory allocation failed, or an error was
   encountered reading the file, this will return NULL.
*/
ZLINE_EXPORT char *ZlineFile_get_line(ZlineFile zf, uint64_t line_idx) {
  int64_t len = ZlineFile_line_length(zf, line_idx);
  char *buf, *result;

  if (len < 0) return NULL;
  
  buf = (char*) malloc(len + 1);
  if (buf == NULL) {
    fprintf(stderr, "Out of memory reading line %" PRIu64 " containing %"
            PRIi64 " bytes\n", line_idx, len);
    return NULL;
  }
  
  result = ZlineFile_get_line2(zf, line_idx, buf, len+1, 0);

  if (result == NULL)
    free(buf);

  return result;
}


/* Like ZlineFile_get_line, but rather than copying the whole line, it
   copies part of line starting 'offset' bytes from its beginning.

   The results are written to 'buf', including a nul terminating
   byte. If the results are longer than buf_len bytes, only the first
   'buf_len'-1 bytes and a nul terminator are copied to 'buf'.

   If 'offset' is past the end of the string, a zero-length string
   is returned.

   Returns NULL on error, otherwise it returns 'buf'.
*/
ZLINE_EXPORT char *ZlineFile_get_line2
  (ZlineFile zf, uint64_t line_idx,
   char *buf, uint64_t buf_len, uint64_t offset) {
  
  ZlineIndexLine *line;
  ZlineBlock *block;
  int64_t file_pos = -1, copy_len;

  assert(zf);

  if (line_idx >= zf->line_count) return NULL;

  zf->fseek_flag = 0;

  /* save the file position in case we need to return to it */
  if (zf->mode == ZLINE_MODE_CREATE) {
    assert(zf->write_block);
    file_pos = zf->write_block->offset;
    /* XXX expensive assert */
    assert(file_pos == ftell(zf->fp));
  }
  
  /* Load the block containing this line, either into write_block
     or read_block. */
  loadLine(zf, line_idx, &line, &block);

  copy_len = 0;

  if (line->length > 0 && offset < line->length) {
    
    copy_len = MIN(line->length - offset, buf_len-1);

    /* If the line was loaded into memory, copy it to the result buffer */
    if (block->content_size > 0) {
      memcpy(buf, block->content + line->offset + offset, copy_len);
    }

    /* if the line was long, loadLine will leave it on disk. Decompress
       it straight to the buffer. */
    else {
      ZlineIndexBlock *index_block;
      i64 compressed_len, decompressed_len;
      assert(block->lines_size == 1);
      assert(block == zf->read_block);
      assert(zf->blocks_size > block->idx);
      
      index_block = zf->blocks + block->idx;
      assert(index_block->offset == block->offset);
      assert(!isBlockLineIndexCompressed(index_block));
      
      fseek(zf->fp, block->offset + block->line_index_size, SEEK_SET);
      compressed_len = getBlockCompressedLen(index_block);
      
      decompressed_len = decompressFromFile(zf, buf, copy_len,
                                            compressed_len, offset);
      if (decompressed_len != (i64)copy_len) {
        fprintf(stderr, "Failed to decompress line %" PRIu64 " from file\n",
                line_idx);
        goto fail;
      }
    }
  }

  buf[copy_len] = 0;

 fail:
  /* restore the file pointer */
  if (zf->fseek_flag && file_pos != -1)
    fseek(zf->fp, file_pos, SEEK_SET);
  
  return buf;
}
              

/* Returns the number of compressed blocks in the file.
   If the file is open in write mode, this may under-report the block
   count by one. */
ZLINE_EXPORT uint64_t ZlineFile_get_block_count(ZlineFile zf) {
  assert(zf);
  return zf->blocks_size;
}


/* Fetches the decompressed size of the given block.
   If block_idx is invalid, returns 0. */
ZLINE_EXPORT uint64_t ZlineFile_get_block_size_original
(ZlineFile zf, uint64_t block_idx) {
  assert(zf);
  if (block_idx >= zf->blocks_size) return 0;
  return zf->blocks[block_idx].decompressed_length;
}


/* Fetches the compressed size of the given block.
   If block_idx is invalid, returns 0. */
ZLINE_EXPORT uint64_t ZlineFile_get_block_size_compressed
(ZlineFile zf, uint64_t block_idx) {
  assert(zf);
  if (block_idx >= zf->blocks_size) return 0;
  return getBlockCompressedLen(zf->blocks + block_idx);
}


/* Returns the index of the first line in the block.
   If block_idx is invalid, returns 0. */
ZLINE_EXPORT uint64_t ZlineFile_get_block_first_line
(ZlineFile zf, uint64_t block_idx) {
  assert(zf);
  if (block_idx >= zf->blocks_size) return 0;

  if (block_idx == 0)
    return 0;
  else
    return zf->block_starts[block_idx - 1];
}


/* Returns the number of lines stored in the block.
   If block_idx is invalid, returns 0. */
ZLINE_EXPORT uint64_t ZlineFile_get_block_line_count
(ZlineFile zf, uint64_t block_idx) {
  u64 block_start, block_end;

  assert(zf);
  if (block_idx >= zf->blocks_size) return 0;

  block_start = (block_idx == 0)
    ? 0
    : zf->block_starts[block_idx-1];
  block_end = (block_idx == zf->blocks_size-1)
    ? zf->line_count
    : zf->block_starts[block_idx];

  return block_end - block_start;
}


ZLINE_EXPORT uint64_t ZlineFile_get_block_offset
  (ZlineFile zf, uint64_t block_idx) {
  if (block_idx >= zf->blocks_size) return 0;
  return zf->blocks[block_idx].offset;
}


/* Return the offset in the file where the block index starts */
ZLINE_EXPORT uint64_t ZlineFile_get_block_index_offset(ZlineFile zf) {
  return zf->index_offset;
}
