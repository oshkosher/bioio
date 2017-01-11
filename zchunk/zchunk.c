#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <inttypes.h>
#include "zchunk.h"

#ifdef ZCHUNK_SUPPORT_GZIP
#include <zlib.h>  /* gzip */
typedef struct gz_state_struct {

  /* zlib (gzip) options
     See http://www.zlib.net/manual.html*/
  z_stream gz;

  /* Z_BEST_COMPRESSION, Z_DEFAULT_COMPRESSION, or Z_BEST_SPEED */
  int zlib_compression_level;

  /* compression window size (1..15), add 16 for gzip stream (rather
     than raw zlib */
  int zlib_window_bits;

  /* 1 (least memory, slow) to 9 (most memory, fast) */  
  int zlib_mem_level;
} gz_state_struct;
#endif

#ifdef ZCHUNK_SUPPORT_BZIP
#include <bzlib.h> /* bzip2 */
typedef struct bz_state_struct {
  /* bzlib (bzip2) options
     See http://www.bzip.org/1.0.5/bzip2-manual-1.0.5.html */

  bz_stream bz;
  
  /* [1..9], increasing memory usage and compression rate */
  int bzlib_block_size;
  
  /* [0..4], silent to verbose */
  int bzlib_verbose;
  
  /* [0..250], affects performance with very repetitive data,
     default is 30, 0 yields the default */
  int bzlib_work_factor;
} bz_state_struct;  
#endif

#ifdef ZCHUNK_SUPPORT_FZSTD
#include "zstd.h"
typedef struct fzstd_state_struct {
  int compression_level;
} fzstd_state_struct;
#endif

#define DEFAULT_INDEX_SIZE 100
#define MAX_ZLIB_WINDOW_BITS 15
#define ZLIB_WINDOW_BITS_GZIP 16



/* Sets defaults in ZChunkEngine, with the algorithm set to gzip with
   maximum compression. */
int zchunkEngineInit(ZChunkEngine *z, ZChunkCompressionAlgorithm alg,
                     ZChunkDirection dir, ZChunkCompressionStrategy strat) {

  memset(z, 0, sizeof(ZChunkEngine));

  if (alg == 0) alg = ZCHUNK_ALG_GZIP;
  if (dir == 0) dir = ZCHUNK_DIR_COMPRESS;
  if (strat == 0) strat = ZCHUNK_STRATEGY_MAX_COMPRESSION;
  
  z->alg = alg;
  z->dir = dir;

#ifdef ZCHUNK_SUPPORT_GZIP
  if (z->alg == ZCHUNK_ALG_GZIP) {
    gz_state_struct *gz;
    int err;
    z->gz_state = gz = (gz_state_struct*) malloc(sizeof(gz_state_struct));
    if (strat == ZCHUNK_STRATEGY_MAX_COMPRESSION) {
      gz->zlib_compression_level = Z_BEST_COMPRESSION;
    } else {
      gz->zlib_compression_level = Z_DEFAULT_COMPRESSION;
    }
    gz->zlib_window_bits = MAX_ZLIB_WINDOW_BITS + ZLIB_WINDOW_BITS_GZIP;
    gz->zlib_mem_level = 9;

    gz->gz.zalloc = Z_NULL;
    gz->gz.zfree = Z_NULL;
    gz->gz.opaque = 0;

    if (dir == ZCHUNK_DIR_COMPRESS) {
      err = deflateInit2(&gz->gz, gz->zlib_compression_level, Z_DEFLATED,
                         gz->zlib_window_bits, gz->zlib_mem_level,
                         Z_DEFAULT_STRATEGY);
      if (err != Z_OK) {
        fprintf(stderr, "Error initializing gzip compression: %s\n",
                err == Z_MEM_ERROR ? "not enough memory"
                : err == Z_STREAM_ERROR ? "invalid compression level"
                : err == Z_VERSION_ERROR ? "invalid library version"
                : "unknown error");
        return 1;
      }
    }

    else { /* dir == ZCHUNK_DIR_DECOMPRESS */
      gz->gz.next_in = Z_NULL;
      gz->gz.avail_in = 0;
      err = inflateInit2(&gz->gz,
                         MAX_ZLIB_WINDOW_BITS + ZLIB_WINDOW_BITS_GZIP);
      if (err != Z_OK) {
        fprintf(stderr, "Error initializing gzip decompression: %s\n",
                err == Z_MEM_ERROR ? "not enough memory"
                : err == Z_STREAM_ERROR ? "invalid compression level"
                : err == Z_VERSION_ERROR ? "invalid library version"
                : "unknown error");
        return 1;
      }
      
    }
  }
#endif

#ifdef ZCHUNK_SUPPORT_BZIP
  if (z->alg == ZCHUNK_ALG_BZIP) {
    bz_state_struct *bz;
    z->bz_state = bz = (bz_state_struct*) malloc(sizeof(bz_state_struct));
    if (strat == ZCHUNK_STRATEGY_MAX_COMPRESSION) {
      bz->bzlib_block_size = 9;
    } else {
      bz->bzlib_block_size = 3;
    }
    bz->bzlib_verbose = 0;
    bz->bzlib_work_factor = 30;

    bz->bz.bzalloc = NULL;
    bz->bz.bzfree = NULL;
    bz->bz.opaque = NULL;
  }
#endif

#ifdef ZCHUNK_SUPPORT_FZSTD
  if (z->alg == ZCHUNK_ALG_FZSTD) {
    fzstd_state_struct *zstd;
    z->fzstd_state = zstd = (fzstd_state_struct*)
      malloc(sizeof(fzstd_state_struct));

    if (strat == ZCHUNK_STRATEGY_MAX_COMPRESSION) {
      zstd->compression_level = 10;
    } else {
      zstd->compression_level = 3;
    }
  }
#endif
  
  return 0;
}



/* With the current settings, return the maximum size buffer that could
   result from compressing n bytes. */
size_t zchunkMaxCompressedSize(ZChunkCompressionAlgorithm alg, size_t n) {
#ifdef ZCHUNK_SUPPORT_GZIP
  if (alg == ZCHUNK_ALG_GZIP) {
    /* Add a bit for gzip header/footer */
    return compressBound(n) + 256;
  }
#endif

  if (alg == ZCHUNK_ALG_BZIP) {
    return n * 101 / 100 + 601;
  }

#ifdef ZCHUNK_SUPPORT_FZSTD
  if (alg == ZCHUNK_ALG_FZSTD) {
    return ZSTD_compressBound(n);
  }
#endif

  return 0;
}


/* Compress or decompress a chunk, depending out how z was initialized. */
size_t zchunkEngineProcess(ZChunkEngine *z,
                           const void *input, size_t input_len,
                           void *output, size_t output_len) {

  int err;
  size_t result_len;
  const char *dir_prefix = z->dir == ZCHUNK_DIR_COMPRESS ? "" : "de";

#ifdef ZCHUNK_SUPPORT_GZIP
  if (z->alg == ZCHUNK_ALG_GZIP) {
    gz_state_struct *gz = z->gz_state;

    gz->gz.next_in = (unsigned char*) input;
    gz->gz.avail_in = input_len;
    gz->gz.next_out = (unsigned char*) output;
    gz->gz.avail_out = output_len;

    if (z->dir == ZCHUNK_DIR_COMPRESS) {
      err = deflate(&gz->gz, Z_FINISH);
      deflateReset(&gz->gz);
    } else {
      err = inflate(&gz->gz, Z_FINISH);
      inflateReset(&gz->gz);
    }

    if (err == Z_STREAM_END) {
      return output_len - gz->gz.avail_out;
    } else {
      fprintf(stderr, "error: %scompressing, error code %d\n",
              dir_prefix, err);
      return 0;
    }

  }
#endif

#ifdef ZCHUNK_SUPPORT_BZIP
  if (z->alg == ZCHUNK_ALG_BZIP) {
    bz_state_struct *bz = z->bz_state;

    /*
    z->bz.bzalloc = NULL;
    z->bz.bzfree = NULL;
    z->bz.opaque = NULL;
    */
    if (z->dir == ZCHUNK_DIR_COMPRESS) {
      err = BZ2_bzCompressInit(&bz->bz, bz->bzlib_block_size,
                               bz->bzlib_verbose, bz->bzlib_work_factor);
    } else {
      err = BZ2_bzDecompressInit(&bz->bz, bz->bzlib_verbose, 0);
    }
    
    if (err != BZ_OK) {
      fprintf(stderr, "Error in bzip %scompress init: %d\n", dir_prefix, err);
      return 0;
    }
  
    bz->bz.next_in = (char*) input;
    bz->bz.avail_in = input_len;
    bz->bz.next_out = (char*) output;
    bz->bz.avail_out = output_len;

    if (z->dir == ZCHUNK_DIR_COMPRESS) {
      err = BZ2_bzCompress(&bz->bz, BZ_FINISH);
    } else {
      err = BZ2_bzDecompress(&bz->bz);
    }
    if (err != BZ_STREAM_END) {
      fprintf(stderr, "Error in bzip %scompress: %d\n", dir_prefix, err);
      return 0;
    }

    result_len = output_len - bz->bz.avail_out;
    if (z->dir == ZCHUNK_DIR_COMPRESS) {
      BZ2_bzCompressEnd(&bz->bz);
    } else {
      BZ2_bzDecompressEnd(&bz->bz);
    }
    return result_len;
  }
#endif

#ifdef ZCHUNK_SUPPORT_FZSTD
  if (z->alg == ZCHUNK_ALG_FZSTD) {
    if (z->dir == ZCHUNK_DIR_COMPRESS) {
      result_len = ZSTD_compress(output, output_len, input, input_len,
                                 z->fzstd_state->compression_level);
    } else {
      result_len = ZSTD_decompress(output, output_len, input, input_len);
    }
    if (ZSTD_isError(result_len)) {
      fprintf(stderr, "error %scompressing: %s\n", dir_prefix,
              ZSTD_getErrorName(result_len));
      return 0;
    } else {
      return result_len;
    }
  }
#endif

  return 0;
}
  


/* Deallocate memory */
void zchunkEngineClose(ZChunkEngine *z) {
  int err;

#ifdef ZCHUNK_SUPPORT_GZIP
  if (z->alg == ZCHUNK_ALG_GZIP) {
    if (z->dir == ZCHUNK_DIR_COMPRESS) {
      err = deflateEnd(&z->gz_state->gz);
    } else {
      err = inflateEnd(&z->gz_state->gz);
    }
    if (err != Z_OK) {
      fprintf(stderr, "zchunkEngineClose error %d\n", err);
    }
  }
#endif
  
  free(z->gz_state);
  free(z->bz_state);
  free(z->fzstd_state);
}


typedef uint64_t u64;

static const u64 FNV_HASH_INIT64 = ((u64)0xcbf29ce4 << 32) | 0x84222325;
static const u64 FNV_HASH_PERMUTE64 = ((u64)0x100 << 32) | 0x000001b3;


uint64_t zchunkHash(const void *data_v, size_t len) {
  const unsigned char *data = (const unsigned char *) data_v;
  u64 hash = FNV_HASH_INIT64;
  size_t i;
  
  for (i=0; i < len; i++) {
    hash = hash ^ data[i];
    hash = hash * FNV_HASH_PERMUTE64;
  }
  return hash;
}


/* Add data to a hash.
   For example:
   uint64_t h = zchunkHash(NULL, 0);
   h = zchunkHashContinue(buf, 100, h);
   h = zchunkHashContinue(buf+100, 25, h);

   would be the same as:
   h = zchunkHash(buf, 125);
 */
uint64_t zchunkHashContinue(const void *data_v, size_t len, uint64_t init) {
  const unsigned char *data = (const unsigned char *) data_v;
  u64 hash = init;
  size_t i;
  
  for (i=0; i < len; i++) {
    hash = hash ^ data[i];
    hash = hash * FNV_HASH_PERMUTE64;
  }
  return hash;
}


void zchunkIndexInit(ZChunkIndex *index) {
  index->capacity = DEFAULT_INDEX_SIZE;
  index->size = 0;
  index->chunks = (ZChunkIndexEntry*)
    malloc(sizeof(ZChunkIndexEntry) * index->capacity);
  assert(index->chunks);
  index->alg = ZCHUNK_ALG_GZIP;
  index->has_hash = 0;
}


int zchunkIndexRead(ZChunkIndex *index, const char *filename) {
  char line[100], *result, word[21];
  int line_no = 0;
  FILE *inf;
  uint64_t prev_z_off = 0, prev_orig_off = 0;

  inf = fopen(filename, "r");
  if (!inf) {
    fprintf(stderr, "Failed to open chunk index \"%s\"\n", filename);
    return 1;
  }

  while (NULL != (result = fgets(line, sizeof(line), inf))) {
    line_no++;

    /* all metadata lines start with a hash */
    if (line[0] == '#') {
      if (!strncmp(line, "# compression: ", 15)) {
        if (1 != sscanf(line+15, "%20s", word)) {
          fprintf(stderr, "Error reading chunk index, line %d, "
                  "expected compression algorithm.\n", line_no);
          goto fail;
        }

        if (word[0] == 'b') {
          index->alg = ZCHUNK_ALG_BZIP;
        } else if (word[0] == 'g') {
          index->alg = ZCHUNK_ALG_GZIP;
        } else if (word[0] == 'f') {
          index->alg = ZCHUNK_ALG_FZSTD;
        } else {
          fprintf(stderr, "Error reading chunk index, line %d, "
                  "invalid compression algorithm \"%s\".\n", line_no, word);
          goto fail;
        }
      }

      else if (!strncmp(line, "# hash: ", 8)) {
        if (1 != sscanf(line+8, "%20s", word)) {
          fprintf(stderr, "Error reading chunk index, line %d, "
                  "expected hash algorithm\n", line_no);
          goto fail;
        }

        if (!strcmp(word, "fnv64")) {
          index->has_hash = 1;
        } else {
          fprintf(stderr, "Error reading chunk index, line %d, "
                  "invalid hash algorithm \"%s\".\n", line_no, word);
          goto fail;
        }
      }
    } /* line[0] == '#' */

    /* the line contains data, not metadata */
    else {
      uint64_t z_off, orig_off, hash = 0;
      
      if (index->has_hash) {
        if (3 != sscanf(line, "%" SCNu64 " %" SCNu64 " %" SCNx64,
                        &z_off, &orig_off, &hash)) {
          fprintf(stderr, "Error reading chunk index, line %d, "
                  "invalid data format\n", line_no);
          goto fail;
        }
      } else {
        if (2 != sscanf(line, "%" SCNu64 " %" SCNu64,
                        &z_off, &orig_off)) {
          fprintf(stderr, "Error reading chunk index, line %d, "
                  "invalid data format\n", line_no);
          goto fail;
        }
      }
      if (orig_off <= prev_orig_off ||
          z_off <= prev_z_off) {
        fprintf(stderr, "Error reading chunk index, line %d, "
                  "out-of-order offsets\n", line_no);
        goto fail;
      }
      zchunkIndexAdd(index, orig_off - prev_orig_off,
                     z_off - prev_z_off, hash);
      prev_orig_off = orig_off;
      prev_z_off = z_off;
    }
                             
  }

  fclose(inf);
  return 0;

 fail:
  fclose(inf);
  return 1;
}



static void zchunkIndexInsureCapacity(ZChunkIndex *index, int cap) {
  if (index->capacity >= cap) return;
  index->capacity *= 2;
  if (index->capacity < cap)
    index->capacity = cap;
  index->chunks = (ZChunkIndexEntry*)
    realloc(index->chunks, index->capacity * sizeof(ZChunkIndexEntry));
}


#ifdef ZCHUNK_MPI
#include "mpi.h"
/* Collective read of ZChunkIndex.
   Rank 0 reads it and broadcasts it.
   Returns nonzero on error. */
int zchunkIndexReadColl(ZChunkIndex *index, const char *filename,
                        MPI_Comm comm) {

  int rank, err;
  int ints[3]; /* size, alg, has_hash */
  
  MPI_Comm_rank(comm, &rank);

  if (rank == 0) {
    err = zchunkIndexRead(index, filename);
    /* failed to read index; send everyone a size of 0 */
    if (err) {
      ints[0] = 0;
      MPI_Bcast(&ints, 3, MPI_INT, 0, comm);
      return 1;
    }

    ints[0] = zchunkIndexSize(index);
    ints[1] = index->alg;
    ints[2] = index->has_hash;
    MPI_Bcast(ints, 3, MPI_INT, 0, comm);
    
  } else {
    MPI_Bcast(ints, 3, MPI_INT, 0, comm);
    if (ints[0] == 0) return 1;

    zchunkIndexInsureCapacity(index, ints[0]);
  
    index->alg = ints[1];
    index->has_hash = ints[2];
    index->size = ints[0];
  }

  /* assert that this shortcut of sending an array of uint64's is valid */
  assert(sizeof(ZChunkIndexEntry) == 3 * 8);

  MPI_Bcast(index->chunks, 3 * index->size, MPI_UINT64_T, 0, comm);

  /*{
    int i;
    printf("[%d] size=%d, cap=%d, alg=%d, has_hash=%d\n",
           rank, index->size, index->capacity, index->alg, index->has_hash);
    for (i=0; i < index->size; i++)
      printf("[%d] %d: %" PRIu64 ", %" PRIu64 ", %" PRIx64 "\n",
             rank, i, index->chunks[i].compressed_end,
             index->chunks[i].original_end,
             index->chunks[i].hash);
  }*/
  
  return 0;
}
#endif
  

void zchunkIndexAdd(ZChunkIndex *index, uint64_t orig_len,
                    uint64_t compressed_len, uint64_t hash) {
  ZChunkIndexEntry *e, *p;

  /* grow the array if necessary */
  zchunkIndexInsureCapacity(index, index->size+1);

  e = index->chunks + index->size;
  e->hash = hash;
  if (index->size == 0) {
    e->compressed_end = compressed_len;
    e->original_end = orig_len;
  } else {
    p = index->chunks + index->size - 1;
    e->compressed_end = p->compressed_end + compressed_len;
    e->original_end = p->original_end + orig_len;
  }

  index->size++;
}

int zchunkIndexSize(ZChunkIndex *index) {
  return index->size;
}

uint64_t zchunkIndexGetOriginalLen(ZChunkIndex *index, int i) {
  if (i == -1) {
    return index->chunks[index->size-1].original_end;
  } else if (i == 0) {
    return index->chunks[0].original_end;
  } else if (i > index->size) {
    return 0;
  } else {
    return index->chunks[i].original_end - index->chunks[i-1].original_end;
  }
}
  
void zchunkIndexGetOrig
(ZChunkIndex *index, int i, uint64_t *offset, uint64_t *len) {
  if (i == 0) {
    *offset = 0;
    *len = index->chunks[0].original_end;
  } else {
    *offset = index->chunks[i-1].original_end;
    *len = index->chunks[i].original_end - *offset;
  }
}


uint64_t zchunkIndexGetHash(ZChunkIndex *index, int i) {
  return index->chunks[i].hash;
}


uint64_t zchunkIndexGetCompressedLen(ZChunkIndex *index, int i) {
  if (i == 0) {
    return index->chunks[0].compressed_end;
  } else {
    return index->chunks[i].compressed_end - index->chunks[i-1].compressed_end;
  }
}


void zchunkIndexGetCompressed
  (ZChunkIndex *index, int i, uint64_t *offset, uint64_t *len,
   uint64_t *hash) {
  *hash = index->chunks[i].hash;
  if (i == 0) {
    *offset = 0;
    *len = index->chunks[0].compressed_end;
  } else {
    *offset = index->chunks[i-1].compressed_end;
    *len = index->chunks[i].compressed_end - *offset;
  }
}


/* Allocate buffers large enough to store the largest chunks in the index. */
int zchunkIndexAllocBuffers(ZChunkIndex *index, void **z_buf, void **o_buf) {
  int n = zchunkIndexSize(index), i;
  uint64_t max_z = 0, max_o = 0, len;
  
  for (i = 0; i < n; i++) {
    len = zchunkIndexGetOriginalLen(index, i);
    if (len > max_o) max_o = len;
    len = zchunkIndexGetCompressedLen(index, i);
    if (len > max_z) max_z = len;
  }

  *z_buf = malloc(max_z);
  *o_buf = malloc(max_o);

  return !*z_buf || !*o_buf;
}


int zchunkIndexWrite(ZChunkIndex *index, const char *filename) {
  int i;
  FILE *outf;

  outf = fopen(filename, "w");
  if (!outf) {
    fprintf(stderr, "Failed to open \"%s\"\n", filename);
    return 1;
  }
  
  fprintf(outf, "# compression: %s\n",
          index->alg == ZCHUNK_ALG_GZIP ? "gzip" :
          index->alg == ZCHUNK_ALG_BZIP ? "bzip2" : "fzstd");
  if (index->has_hash)
    fprintf(outf, "# hash: fnv64\n");

  for (i=0; i < index->size; i++) {
    if (index->has_hash)
      fprintf(outf, "%" PRIu64 "\t%" PRIu64 "\t%016" PRIx64 "\n",
              index->chunks[i].compressed_end,
              index->chunks[i].original_end, 
              index->chunks[i].hash);
    else
      fprintf(outf, "%" PRIu64 "\t%" PRIu64 "\n",
              index->chunks[i].compressed_end,
              index->chunks[i].original_end);
  }

  fclose(outf);
  return 0;
}


/* Figure out which chunks are needed to extract the given range.
   offset - offset into the original file of the desired range
   len - length of desired range
   z_offset - offset into the compressed file where the needed chunks start
   z_len - length of the needed compressed chunks
   uz_full_len - uncompressed length of the needed chunks (may be > len)
   uz_offset - after decompressing, this is the offset in the decompressed
     data where the desired range can be found

   Returns 0 on success.
   Returns -1 if the target range is past the end of the file.
 */
int zchunkIndexRange(ZChunkIndex *index,
                     uint64_t offset, uint64_t len,
                     uint64_t *z_offset, uint64_t *z_len,
                     uint64_t *uz_full_len, uint64_t *uz_offset) {
  
  int first_chunk, last_chunk;
  uint64_t first_chunk_o_offset;

  /* check if the range is past the end of the file */
  if (offset+len > index->chunks[index->size-1].original_end)
    return -1;

  first_chunk = 0;
  while (index->chunks[first_chunk].original_end <= offset)
    first_chunk++;

  last_chunk = first_chunk;
  while (index->chunks[last_chunk].original_end < offset + len)
    last_chunk++;

  first_chunk_o_offset =
    (first_chunk == 0) ? 0 : index->chunks[first_chunk-1].original_end;

  *z_offset =
    (first_chunk == 0) ? 0 : index->chunks[first_chunk-1].compressed_end;
  *z_len = index->chunks[last_chunk].compressed_end - *z_offset;

  *uz_full_len = index->chunks[last_chunk].original_end - first_chunk_o_offset;
  *uz_offset = offset - first_chunk_o_offset;
  
  return 0;
}


void zchunkIndexClose(ZChunkIndex *index) {
  free(index->chunks);
}

#ifdef ZCHUNK_MPI

ZChunkFileMPI *ZChunkFileMPI_create
(const char *data_file, const char *index_file,
 MPI_Comm comm, ZChunkCompressionAlgorithm alg, int do_hash,
 int stripe_count, uint64_t stripe_size) {
  MPI_Info info;
  char stripe_count_str[50], stripe_size_str[50];
  int err;
  ZChunkFileMPI *f;

  f = (ZChunkFileMPI*) calloc(1, sizeof(ZChunkFileMPI));
  f->is_creating = 1;
  f->index_file_name = index_file;
  
  zchunkIndexInit(&f->index);
  zchunkEngineInit(&f->zip, alg, ZCHUNK_DIR_COMPRESS,
                   ZCHUNK_STRATEGY_MAX_COMPRESSION);

  f->index.has_hash = do_hash;
  f->comm = comm;
  MPI_Comm_rank(comm, &f->rank);
  MPI_Comm_size(comm, &f->np);

  if (f->rank == 0) {
    f->write_offset_array = (u64*) malloc(sizeof(u64) * 4 * f->np);
    assert(f->write_offset_array);
  } else {
    f->write_offset_array = 0;
  }

  
  sprintf(stripe_count_str, "%d", stripe_count);
  sprintf(stripe_size_str, "%lu", stripe_size);

  MPI_Info_create(&info);
  MPI_Info_set(info, "striping_factor", stripe_count_str);
  MPI_Info_set(info, "striping_unit", stripe_size_str);

  /* delete the file first, because we can't set the striping if it exists */
  if (f->rank == 0)
    MPI_File_delete(data_file, MPI_INFO_NULL);

  /* wait for rank 0 to delete the file */
  MPI_Barrier(f->comm);
  
  err = MPI_File_open(f->comm, data_file,
                      MPI_MODE_WRONLY | MPI_MODE_UNIQUE_OPEN | MPI_MODE_CREATE,
                      info, &f->f);
  if (err != MPI_SUCCESS) {
    if (f->rank == 0) {
      fprintf(stderr, "Failed to open output file %s: error %d\n", data_file,
              err);
    }
    return NULL;
  }
  MPI_Info_free(&info);

  /* Truncate the output file--we'll completely rewrite it */
  MPI_File_set_size(f->f, 0);
  
  return f;
}


/* Open the file for reading */
ZChunkFileMPI *ZChunkFileMPI_open(const char *data_file, const char *index_file,
                                  MPI_Comm comm) {
  int err;
  ZChunkFileMPI *f;
  
  f = (ZChunkFileMPI*) calloc(1, sizeof(ZChunkFileMPI));
  f->is_creating = 0;
  f->index_file_name = index_file;
  f->comm = comm;
  MPI_Comm_rank(comm, &f->rank);
  MPI_Comm_size(comm, &f->np);
  f->write_offset_array = NULL;
  f->write_pos = 0;
  
  /* read the index */
  err = zchunkIndexReadColl(&f->index, index_file, comm);
  if (err) goto fail0;
  
  err = zchunkEngineInit(&f->zip, f->index.alg, ZCHUNK_DIR_DECOMPRESS, 0);
  if (err) goto fail1;
  
  err = MPI_File_open(f->comm, data_file,
                      MPI_MODE_RDONLY | MPI_MODE_UNIQUE_OPEN,
                      MPI_INFO_NULL, &f->f);
  if (err != MPI_SUCCESS) {
    if (f->rank == 0) {
      fprintf(stderr, "Failed to open output file %s: error %d\n", data_file,
              err);
    }
    goto fail2;
  }
  
  return f;

 fail2:
  zchunkEngineClose(&f->zip);
 fail1:
  zchunkIndexClose(&f->index);
  free(f);
 fail0:
  return NULL;
}  



static void insureCapacity(void **buf, size_t *buf_size, size_t size) {
  /* If the buffer hasn't been allocated, do it now */
  if (*buf) {
    *buf_size = size;
    *buf = malloc(size);
    return;
  }

  /* if the buffer is big enough, do nothing */
  if (size <= *buf_size) return;

  /* avoid doing a zillion small reallocations by at least doubling the size */
  *buf_size *= 2;
  if (size > *buf_size) *buf_size = size;

  *buf = realloc(*buf, *buf_size);
}


/* If buf is NULL or len is 0, participate in the collective operations,
   buf don't contribute any data.

   Returns nonzero on error.
*/
int ZChunkFileMPI_append_all(ZChunkFileMPI *f, const void *buf, uint64_t len) {
  u64 compressed_len, cumulative_len, hash = 0;
  MPI_Status status;
  int write_len, i;
  u64 exchange_data[3];

  if (!f->is_creating) {
    fprintf(stderr, "ERROR: ZChunkFileMPI is opened for reading, "
            "not writing\n");
    return -1;
  }
  
  if (f->index.has_hash) {
    hash = zchunkHash(buf, len);
  }

  /* compress this chunk */
  if (buf && len) {
    insureCapacity(&f->buf, &f->buf_size,
                   zchunkMaxCompressedSize(f->zip.alg, len));
    compressed_len = zchunkEngineProcess(&f->zip, buf, len, f->buf,
                                         f->buf_size);
  } else {
    compressed_len = 0;
  }

  /* do a prefix sum to figure out where to write my chunk */
  cumulative_len = compressed_len;
  MPI_Scan(MPI_IN_PLACE, &cumulative_len, 1, MPI_UINT64_T, MPI_SUM, f->comm);
  f->write_pos += cumulative_len - compressed_len;

  /* write all the chunks */
  MPI_File_write_at_all(f->f, f->write_pos, f->buf, compressed_len,
                        MPI_BYTE, &status);
  MPI_Get_count(&status, MPI_BYTE, &write_len);
  if (write_len != compressed_len) {
    fprintf(stderr, "[%d] write failure: only %d of %" PRIu64
            " bytes written\n",
            f->rank, write_len, compressed_len);
    return 1;
  }

  /* gather chunk info on the root process */
  exchange_data[0] = len;
  exchange_data[1] = compressed_len;
  exchange_data[2] = hash;
  MPI_Gather(exchange_data, 3, MPI_UINT64_T, f->write_offset_array,
             3, MPI_UINT64_T, 0, f->comm);
  
  /* root process add chunk info to the index */
  if (f->rank == 0) {
    for (i=0; i < f->np; i++) {
      u64 *p = f->write_offset_array + (i * 3);

      /* skip if the length is 0 */
      if (p[1] != 0) {
        zchunkIndexAdd(&f->index, p[0], p[1], p[2]);
      }
    }
  }

  f->write_pos += compressed_len;

  /* The last rank knows where its data ended, so have it tell everyone
     where next chunks should go. */
  MPI_Bcast(&f->write_pos, 1, MPI_UINT64_T, f->np-1, f->comm);

  return 0;
}


uint64_t ZChunkFileMPI_get_length(ZChunkFileMPI *f) {
  return zchunkIndexGetOriginalLen(&f->index, -1);
}


static int ZChunkFileMPI_read_at_internal
(ZChunkFileMPI *f, void *buf, uint64_t offset,
 uint64_t len, int is_collective) {
  int err, bytes_read,
    direct_decompress; /* if the data is aligned, this will be set and
                          a memcpy will be avoided */
  u64 z_offset, z_len,  /* the compressed data that will be read */
    uz_offset, /* after decompressing, the offset of the desired data */
    uz_len, /* length of the data after decompressing. */
    uz_len_result;
  MPI_Status status;
  double start_time;

  /* if the requested data does not align with chunks in the file, 
     uz_offset might be nonzero and uz_len may be > len */
    
  if (f->is_creating) {
    fprintf(stderr, "ERROR: ZChunkFileMPI is opened for writing, "
            "not reading\n");
    return -1;
  }

  err = zchunkIndexRange(&f->index, offset, len, &z_offset, &z_len,
                         &uz_len, &uz_offset);
  if (err) {
    fprintf(stderr, "ERROR bad ZChunkFileMPI_read_at call for %" PRIu64
            " bytes at offset %" PRIu64 " with %" PRIu64 "bytes of data\n",
            len, offset, ZChunkFileMPI_get_length(f));
    return -1;
  }

  /* debug output */
  printf("[%d] zchunkIndexRange offset %" PRIu64 " len %" PRIu64 ", z_offset=%" PRIu64 " z_len=%" PRIu64 " uz_len=%" PRIu64 " uz_offset=%" PRIu64 "\n", f->rank, offset, len, z_offset, z_len, uz_len, uz_offset);

  /* check if we can decompress directly into the user's buffer */
  direct_decompress = (uz_len == len && uz_offset == 0);
  
  insureCapacity(&f->iobuf, &f->iobuf_size, z_len);

  if (!direct_decompress)
    insureCapacity(&f->buf, &f->buf_size, uz_len);

  /* read the compressed data */
  start_time = MPI_Wtime();
  if (is_collective) {
    MPI_File_read_at_all(f->f, z_offset, f->iobuf, z_len, MPI_BYTE, &status);
  } else {
    MPI_File_read_at(f->f, z_offset, f->iobuf, z_len, MPI_BYTE, &status);
  }
  f->time_reading += MPI_Wtime() - start_time;
  MPI_Get_count(&status, MPI_BYTE, &bytes_read);
  if (bytes_read != z_len) {
    fprintf(stderr, "[%d] read length mismatch. At offset %" PRIu64
            ", wanted %" PRIu64 " bytes, got %d.\n",
            f->rank, z_offset, z_len, bytes_read);
  }

  /* decompress it */
  start_time = MPI_Wtime();
  uz_len_result = zchunkEngineProcess
    (&f->zip, f->iobuf, z_len, direct_decompress ? buf : f->buf, uz_len);
  f->time_decompressing += MPI_Wtime() - start_time;
  if (uz_len_result != uz_len) {
    fprintf(stderr, "ERROR: decompressed length expected %lu, got %lu\n",
            (unsigned long)uz_len, (unsigned long)uz_len_result);
    return -1;
  }

  /* if the data wasn't decompressed directly into the user's buffer, copy
     it there */
  if (!direct_decompress) {
    memcpy(buf, f->buf + uz_offset, len);
  }

  return 0;
}


/* Read one chunk of data.
   Read the compressed chunks covering the required data, decompress them,
   and copy the decompressed data into the given buffer. */
int ZChunkFileMPI_read_at(ZChunkFileMPI *f, void *buf, uint64_t offset,
                          uint64_t len) {
  return ZChunkFileMPI_read_at_internal(f, buf, offset, len, 0);
}


/* Collectively read one chunk of data.
   This is a collective call for every process in f->comm.
*/
int ZChunkFileMPI_read_at_all(ZChunkFileMPI *f, void *buf, uint64_t offset,
                          uint64_t len) {
  return ZChunkFileMPI_read_at_internal(f, buf, offset, len, 1);
}  


void ZChunkFileMPI_close(ZChunkFileMPI *f) {
  MPI_File_close(&f->f);

  /* write index */
  if (f->is_creating && f->rank == 0) {
    zchunkIndexWrite(&f->index, f->index_file_name);
    zchunkIndexClose(&f->index);
  }
  
  zchunkEngineClose(&f->zip);
  free(f->write_offset_array);
  free(f->buf);
  free(f);
}


#endif /* ZCHUNK_MPI */
