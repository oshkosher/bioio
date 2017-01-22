#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <inttypes.h>
#include "zline_api.h"
#include "zstd.h"

#define INITIAL_BLOCK_CAPACITY 100
#define INITIAL_LINE_CAPACITY 1000
#define HEADER_SIZE 256
#define ZSTD_COMPRESSION_LEVEL 3
#define MAX_HEADER_LINE_LEN 100

#define ZLINE_MODE_CREATE 1
#define ZLINE_MODE_READ 2

#define MIN(a,b) ((a)<(b)?(a):(b))
#define MAX(a,b) ((a)>(b)?(a):(b))

typedef uint64_t u64;

static int writeHeader(ZlineFile *zf);
static int readHeader(ZlineFile *zf);
static void blocksInsureCapacity(ZlineFile *zf, u64 capacity);
static void linesInsureCapacity(ZlineFile *zf, u64 capacity);

/* read a block, decompress it, store the decompressed result in outbuf,
   store the decompressed size in outbuf_size, and store the index of the block
   in outbuf_idx. */
static int getBlock(ZlineFile *zf, u64 block_idx);


ZlineFile *ZlineFile_create(const char *output_filename, int block_size) {
  ZlineFile *zf = (ZlineFile*) calloc(1, sizeof(ZlineFile));
  if (!zf) {
    fprintf(stderr, "Out of memory\n");
    return NULL;
  }

  zf->filename = output_filename;
  zf->mode = ZLINE_MODE_CREATE;
  zf->data_offset = HEADER_SIZE;
  zf->inbuf_capacity = block_size;
  zf->inbuf = (char*) malloc(zf->inbuf_capacity);
  if (!zf->inbuf) {
    fprintf(stderr, "Out of memory\n");
    goto fail;
  }

  /* allocate a buffer big enough to fit the largest possible compressed
     version of zf->inbuf. */
  zf->outbuf_capacity = ZSTD_compressBound(zf->inbuf_capacity);
  zf->outbuf = (char*) malloc(zf->outbuf_capacity);
  if (!zf->outbuf) {
    fprintf(stderr, "Out of memory\n");
    goto fail;
  }
  zf->outbuf_idx = UINT64_MAX;

  /* open the output file */
  zf->fp = fopen(output_filename, "wb");
  if (!zf->fp) goto fail;

  if (writeHeader(zf)) goto fail;

  blocksInsureCapacity(zf, INITIAL_BLOCK_CAPACITY);
  linesInsureCapacity(zf, INITIAL_LINE_CAPACITY);

  zf->blocks[0].offset = HEADER_SIZE;
  
  return zf;

 fail:
  if (zf->fp) fclose(zf->fp);
  free(zf->inbuf);
  free(zf->outbuf);
  free(zf->blocks);
  free(zf->lines);
  free(zf);
  return NULL;
}

static int writeHeader(ZlineFile *zf) {
  char buf[HEADER_SIZE];
  int pos = 0;
  size_t write_len;

  pos += sprintf(buf, "zline v1.0\n");
  pos += sprintf(buf+pos, "data_offset %" PRIu64 "\n", zf->data_offset);
  pos += sprintf(buf+pos, "index_offset %" PRIu64 "\n", zf->index_offset);
  pos += sprintf(buf+pos, "lines %" PRIu64 "\n", zf->line_count);
  pos += sprintf(buf+pos, "blocks %" PRIu64 "\n", zf->block_count);
  pos += sprintf(buf+pos, "alg fzstd\n");
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


static int readHeader(ZlineFile *zf) {
  char buf[MAX_HEADER_LINE_LEN], word[MAX_HEADER_LINE_LEN];

  assert(zf->fp);

  if (fseek(zf->fp, 0, SEEK_SET)) {
    fprintf(stderr, "Failed to move to top of file to read header.\n");
    return 1;
  }

  if (!fgets(buf, MAX_HEADER_LINE_LEN, zf->fp))
    goto format_error;
  
  if (strncmp(buf, "zline v1.0", 10))
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
      if (1 != sscanf(buf+pos, "%" SCNu64, &zf->block_count)) goto format_error;
    } else if (!strcmp(word, "alg")) {
      if (1 != sscanf(buf+pos, "%s", word)) goto format_error;
      if (strcmp(word, "fzstd")) {
        fprintf(stderr, "Unrecognized compression algorithm: \"%s\"\n", word);
        return 1;
      }
    } else {
      goto format_error;
    }
  }

  if (zf->data_offset == 0 ||
      zf->index_offset == 0 ||
      zf->line_count == 0 ||
      zf->block_count == 0) {
    fprintf(stderr, "File header incomplete\n");
    return 1;
  }
  
  return 0;

 format_error:
  fprintf(stderr, "Error reading \"%s\", invalid format\n", zf->filename);
  return 1;
}


static void addLineInternal(ZlineFile *zf, u64 block_idx, u64 offset,
                            u64 length) {
  ZlineIndexLine *line;
  
  linesInsureCapacity(zf, zf->line_count + 1);
  line = zf->lines + zf->line_count;
  line->block_idx = block_idx;
  line->offset = offset;
  line->length = length;

  /*
  printf("line %d: block %d, offset %d, length %d\n",
         (int)zf->line_count, (int)block_idx, (int)line->offset,
         (int)line->length);
  */
  
  zf->line_count++;
}


static int flushBlock(ZlineFile *zf) {
  ZlineIndexBlock *block = zf->blocks + zf->block_count;
  size_t write_len;
  u64 next_block_start;

  assert(block->offset > 0);
  block->decompressed_length = zf->inbuf_size;

  /* compress the block */
  block->compressed_length = ZSTD_compress
    (zf->outbuf, zf->outbuf_capacity, zf->inbuf, zf->inbuf_size,
     ZSTD_COMPRESSION_LEVEL);
  
  assert(block->compressed_length > 0);
  zf->outbuf_size = block->compressed_length;
  next_block_start = block->offset + block->compressed_length;

  printf("block %" PRIu64 " %d bytes -> %d bytes\n", zf->block_count,
         zf->inbuf_size, zf->outbuf_size);

  /* check we're at the expected offset in the file */
  /* XXX slow assert */
  /* assert(ftell(zf->fp) == block->offset); */

  /* write the compressed data to the file */
  write_len = fwrite(zf->outbuf, 1, zf->outbuf_size, zf->fp);

  /*
  printf("block %d at %d, len %d\n", (int)zf->block_count,
         (int)block->offset, (int)block->compressed_length);
  */

  /* move to the next block and initialize its offset */
  blocksInsureCapacity(zf, zf->block_count + 2);
  zf->block_count++;
  zf->blocks[zf->block_count].offset = next_block_start;
  zf->inbuf_size = 0;

  if (write_len != (size_t)zf->outbuf_size) {
    fprintf(stderr, "Failed to write %d bytes to \"%s\" at offset "
            "%" PRIu64 "\n", zf->outbuf_size, zf->filename,
            zf->blocks[zf->block_count-1].offset);
    return 1;
  } else {
    return 0;
  }
}
            
  
  
/* length is the length of the line, and is optional. If 0, it will be
   computed using strlen. */
int ZlineFile_add_line(ZlineFile *zf, const char *line, uint64_t length) {

  if (zf->mode != ZLINE_MODE_CREATE) {
    fprintf(stderr, "\"%s\" not opened for writing\n", zf->filename);
    return -1;
  }
  
  if (length == 0) length = strlen(line);

  /* the line doesn't fit in the current block, flush the current block */
  if (zf->inbuf_size + length > (u64)zf->inbuf_capacity) {
    flushBlock(zf);
  }

  /* add an entry to the line index */
  addLineInternal(zf, zf->block_count, zf->inbuf_size, length);

  while (length) {
    uint64_t bite = MIN(length, (u64)(zf->inbuf_capacity - zf->inbuf_size));
    memcpy(zf->inbuf + zf->inbuf_size, line, bite);
    zf->inbuf_size += bite;
    length -= bite;
    line += bite;

    if (length) flushBlock(zf);
  }

  return 0;
}

  
void ZlineFile_close(ZlineFile *zf) {
  size_t write_len;
  int pad_size;
  char pad_buf[7] = {0};
  u64 current_pos;

  if (zf->mode == ZLINE_MODE_READ) goto ok;
  
  if (zf->inbuf_size > 0)
    flushBlock(zf);

  current_pos = zf->blocks[zf->block_count].offset;
  
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
  write_len = fwrite(zf->blocks, sizeof(ZlineIndexBlock),
                     zf->block_count, zf->fp);
  if (write_len != zf->block_count) goto fail;

  write_len = fwrite(zf->lines, sizeof(ZlineIndexLine),
                     zf->line_count, zf->fp);
  if (write_len != zf->line_count) goto fail;

  /* write the header */
  if (writeHeader(zf)) goto fail;

  /* close the file */
    goto ok;
 fail:
    fprintf(stderr, "Error writing index to \"%s\"\n", zf->filename);
 ok:

  /* deallocate everything */
  if (zf->fp) fclose(zf->fp);
  free(zf->inbuf);
  free(zf->outbuf);
  free(zf->blocks);
  free(zf->lines);
  free(zf);
}


static void blocksInsureCapacity(ZlineFile *zf, u64 capacity) {
  u64 new_cap;
  if (!zf->blocks || zf->blocks_capacity < capacity) {
    new_cap = zf->blocks_capacity * 2;
    if (new_cap < capacity) new_cap = capacity;
    zf->blocks = (ZlineIndexBlock*) realloc
      (zf->blocks, sizeof(ZlineIndexBlock) * new_cap);
    if (!zf->blocks) {
      fprintf(stderr, "Out of memory allocating block array\n");
      assert(0);
    }
    zf->blocks_capacity = new_cap;
  }
}

  
static void linesInsureCapacity(ZlineFile *zf, u64 capacity) {
  u64 new_cap;
  if (!zf->lines || zf->lines_capacity < capacity) {
    new_cap = zf->lines_capacity * 2;
    if (new_cap < capacity) new_cap = capacity;
    zf->lines = (ZlineIndexLine*) realloc
      (zf->lines, sizeof(ZlineIndexLine) * new_cap);
    if (!zf->lines) {
      fprintf(stderr, "Out of memory allocating line array\n");
      assert(0);
    }
    zf->lines_capacity = new_cap;
  }
}


/* Open an existing zlines file for reading. */
ZlineFile *ZlineFile_read(const char *filename) {
  ZlineFile *zf = (ZlineFile*) calloc(1, sizeof(ZlineFile));
  size_t read_len;
  u64 max_c = 0, max_dc = 0;
  ZlineIndexBlock *block;
  
  assert(zf);

  zf->filename = filename;
  zf->mode = ZLINE_MODE_READ;
  zf->outbuf_idx = UINT64_MAX;

  zf->fp = fopen(filename, "rb");
  if (!zf->fp) goto fail;

  /* read the header */
  if (readHeader(zf)) goto fail;

  /* allocate space for the index */
  blocksInsureCapacity(zf, zf->block_count);
  linesInsureCapacity(zf, zf->line_count);

  if (fseek(zf->fp, zf->index_offset, SEEK_SET)) goto fail;

  /* read the index */
  read_len = fread(zf->blocks, sizeof(ZlineIndexBlock), zf->block_count, zf->fp);
  if (read_len != zf->block_count) goto fail;

  read_len = fread(zf->lines, sizeof(ZlineIndexLine), zf->line_count, zf->fp);
  if (read_len != zf->line_count) goto fail;

  /* scan the index to find the largest compressed and decompressed blocks */
  for (block = zf->blocks; block < zf->blocks + zf->block_count; block++) {
    max_c = MAX(max_c, block->compressed_length);
    max_dc = MAX(max_dc, block->decompressed_length);
  }

  zf->inbuf = (char*) malloc(max_c);
  zf->outbuf = (char*) malloc(max_dc);
  if (!zf->inbuf || !zf->outbuf) {
    fprintf(stderr, "Out of memory opening file\n");
    goto fail;
  }
  zf->inbuf_capacity = max_c;
  zf->outbuf_capacity = max_dc;

  return zf;

 fail:
  if (zf->fp) fclose(zf->fp);
  free(zf->inbuf);
  free(zf->outbuf);
  free(zf->blocks);
  free(zf->lines);
  free(zf);
  return NULL;
}


/* Returns the number of lines in the file. */
uint64_t ZlineFile_line_count(ZlineFile *zf) {
  return zf->line_count;
}

/* Returns the length of the given line. */
uint64_t ZlineFile_line_length(ZlineFile *zf, uint64_t line_idx) {
  if (line_idx >= zf->line_count)
    return 0;
  else
    return zf->lines[line_idx].length;
}

/* Returns the length of the longest line. */
uint64_t ZlineFile_max_line_length(ZlineFile *zf) {
  u64 i, max_len = 0;
  for (i=0; i < zf->line_count; i++)
    max_len = MAX(max_len, zf->lines[i].length);
  return max_len;
}

/* read a block, decompress it, store the decompressed result in outbuf,
   store the decompressed size in outbuf_size, and store the index of the block
   in outbuf_idx. */
static int getBlock(ZlineFile *zf, u64 block_idx) {
  ZlineIndexBlock *block;
  size_t dc_len;
  assert(zf->mode == ZLINE_MODE_READ);
  assert(block_idx < zf->block_count);

  /* check if we already have this block decompressed */
  if (block_idx == zf->outbuf_idx) return 0;

  block = zf->blocks + block_idx;
  if (fseek(zf->fp, block->offset, SEEK_SET)) {
    fprintf(stderr, "Failed to seek to block %" PRIu64 " offset %" PRIu64 "\n",
            block_idx, block->offset);
    return 1;
  }

  assert((u64)zf->inbuf_capacity >= block->compressed_length);
  assert((u64)zf->outbuf_capacity >= block->decompressed_length);

  if (1 != fread(zf->inbuf, block->compressed_length, 1, zf->fp)) {
    fprintf(stderr, "Failed to read block %" PRIu64 "\n", block_idx);
    return 1;
  }
  zf->inbuf_size = block->compressed_length;

  dc_len = ZSTD_decompress(zf->outbuf, zf->outbuf_capacity,
                           zf->inbuf, zf->inbuf_size);
  if (dc_len != block->decompressed_length) {
    fprintf(stderr, "Decompression error block %" PRIu64 ": "
            "expected %" PRIu64 " bytes, got %" PRIu64 "\n",
            block_idx, block->decompressed_length, (u64) dc_len);
    return 1;
  }
  zf->outbuf_size = block->decompressed_length;
  zf->outbuf_idx = block_idx;

  return 0;
}


/* Reads a line from the file. If buf is NULL, memory to store the line will
   be allocated with malloc. The caller must deallocate the memory with free().
   If buf is not NULL, the line will be written to that location.
   The caller can use ZlineFile_line_length() to check that their buffer
   is large enough.

   Return value: The location where the line was written, or NULL on error.

   If line_idx is >= ZlineFile_line_count(zf), or a memory allocation failed,
   or an error was encountered reading the file, this will return NULL.
*/
char *ZlineFile_get_line(ZlineFile *zf, uint64_t line_idx, char *buf) {
  ZlineIndexLine *line;
  u64 file_pos = 0;
  char *result = NULL;

  assert(zf);

  /* save my place in the file if the file is currently being written */
  if (zf->mode == ZLINE_MODE_CREATE)
    file_pos = ftell(zf->fp);
  
  if (line_idx >= zf->line_count) goto fail;

  line = zf->lines + line_idx;

  /* read and decompress the block containing this line */
  if (getBlock(zf, line->block_idx)) goto fail;
  
  if (!buf) {
    buf = (char*) malloc(line->length + 1);
    if (!buf) {
      fprintf(stderr, "Out of memory getting line %" PRIu64 ", length %" PRIu64 "\n",
              line_idx, line->length);
      goto fail;
    }
  }

  memcpy(buf, zf->outbuf + line->offset, line->length);
  buf[line->length] = 0;
  result = buf;

 fail:
  if (zf->mode == ZLINE_MODE_CREATE)
    fseek(zf->fp, file_pos, SEEK_SET);
  
  return result;
}
              

