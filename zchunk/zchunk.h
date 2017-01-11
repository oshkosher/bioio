#ifndef __ZCHUNK_H__
#define __ZCHUNK_H__

#include <stdio.h>
#include <stdint.h>
#ifdef ZCHUNK_MPI
#include "mpi.h"
#endif


typedef enum {
  ZCHUNK_ALG_GZIP = 1, /* gzip */
  ZCHUNK_ALG_BZIP = 2, /* bzip2 */
  ZCHUNK_ALG_FZSTD = 3  /* Facebook's ZSTD */
} ZChunkCompressionAlgorithm;

typedef enum {
  ZCHUNK_DIR_COMPRESS = 1, 
  ZCHUNK_DIR_DECOMPRESS = 2
} ZChunkDirection;

typedef enum {
  ZCHUNK_STRATEGY_MAX_COMPRESSION = 1, 
  ZCHUNK_STRATEGY_FAST = 2
} ZChunkCompressionStrategy;


struct gz_state_struct;
struct bz_state_struct;
struct fzstd_state_struct;

/* Options for compression and decompression.
   Support gzip or bzip2 compression.
   bzip2 tends to get better compression ratios, but tends to decompress faster.

   The only field the user should change is "alg".

   Call zchunkEngineInit() to initialize everything to default values.
*/
typedef struct ZChunkEngine {

  /* ZCHUNK_ALG_GZIP for gzip, ZCHUNK_ALG_BZIP for bzip2 */
  ZChunkCompressionAlgorithm alg;

  /* The rest of the fields are for internal use only */

  /* NONE: not initialized
     COMPRESS: gz or bz have initialized for compression
     DECOMPRESS: gz or bz have initialized for decompression
  */
  ZChunkDirection dir;

  struct gz_state_struct *gz_state;
  struct bz_state_struct *bz_state;
  struct fzstd_state_struct *fzstd_state;
  
} ZChunkEngine;


/* Each instance of this is one chunk of data. */
typedef struct {
  /* This is the offset just after the last byte of the data for this chunk
     in the compressed data. */
  uint64_t compressed_end;

  /* This is the offset just after the last byte of the data for this chunk
     in the original file. */
  uint64_t original_end;

  /* Hash of the data. If hashes were not computed, this will be 0. */
  uint64_t hash;
} ZChunkIndexEntry;


typedef struct {
  ZChunkIndexEntry *chunks;
  int size, capacity; /* of the chunks array */
  
  ZChunkCompressionAlgorithm alg;
  int has_hash;
} ZChunkIndex;


/* With the given algorithm, return the maximum size buffer that could
   result from compressing n bytes. */
size_t zchunkMaxCompressedSize(ZChunkCompressionAlgorithm alg, size_t n);

/* Sets defaults in ZChunkEngine, with the algorithm set to bzip2.
   Zero values for alg, dir, or strat yields default values.
   Returns nonzero and prints error message to stderr on error. */
int zchunkEngineInit(ZChunkEngine *z, ZChunkCompressionAlgorithm alg,
                     ZChunkDirection dir, ZChunkCompressionStrategy strat);

/* Compress or decompress a chunk, depending out how z was initialized.
   Returns the number of bytes written to 'output'. */
size_t zchunkEngineProcess(ZChunkEngine *z,
                           const void *input, size_t input_len,
                           void *output, size_t output_len);

/* Deallocate memory */
void zchunkEngineClose(ZChunkEngine *z);


/* 64 bit FNV hash, provided for convenience. This is a non-cryptographic hash
   with a simple implementation. */
uint64_t zchunkHash(const void *buf, size_t len);

uint64_t zchunkHashContinue(const void *data_v, size_t len, uint64_t init);


/* Routines for accessing the index for a bunch of zchunks. */

void zchunkIndexInit(ZChunkIndex *index);

/* Read an index (which has already been initialized) from a file */
int zchunkIndexRead(ZChunkIndex *index, const char *filename);

#ifdef ZCHUNK_MPI
#include "mpi.h"
/* Collective read of ZChunkIndex
   Rank 0 reads it and broadcasts it. */
int zchunkIndexReadColl(ZChunkIndex *index, const char *filename,
                        MPI_Comm comm);
#endif

void zchunkIndexAdd(ZChunkIndex *index, uint64_t orig_len,
                    uint64_t compressed_len, uint64_t hash);

/* number of chunks in the index */
int zchunkIndexSize(ZChunkIndex *index);

/* Returns the length of this chunk when uncompressed.
   If i==-1, returns the total length of all uncompressed chunks. */
uint64_t zchunkIndexGetOriginalLen(ZChunkIndex *index, int i);

void zchunkIndexGetOrig
(ZChunkIndex *index, int i, uint64_t *offset, uint64_t *len);

uint64_t zchunkIndexGetHash(ZChunkIndex *index, int i);

uint64_t zchunkIndexGetCompressedLen(ZChunkIndex *index, int i);
void zchunkIndexGetCompressed
(ZChunkIndex *index, int i, uint64_t *offset, uint64_t *len,
 uint64_t *hash);

/* Allocate buffers large enough to store the largest chunks in the index. */
int zchunkIndexAllocBuffers(ZChunkIndex *index, void **z_buf, void **o_buf);

/* Return nonzero on error. */
int zchunkIndexWrite(ZChunkIndex *index, const char *filename);

/* Figure out which chunks are needed to extract the given range.
   offset - offset into the original file of the desired range
   len - length of desired range
   z_offset - offset into the compressed file where the needed chunks start
   z_len - length of the needed compressed chunks
   uz_full_len - uncompressed length of the needed chunks (may be > len)
   uz_offset - after decompressing, this is the offset in the decompressed
     data where the desired range can be found
 */
int zchunkIndexRange(ZChunkIndex *index,
                     uint64_t offset, uint64_t len,
                     uint64_t *z_offset, uint64_t *z_len,
                     uint64_t *uz_full_len, uint64_t *uz_offset);

void zchunkIndexClose(ZChunkIndex *index);


/* Highest-level API

   Write and read zchunk files like FILE*'s or MPI_File's.
*/

typedef struct {
  ZChunkIndex index;
  ZChunkEngine zip;
} ZChunkFileBase;

typedef struct {
  ZChunkIndex index;
  ZChunkEngine zip;
  FILE *f;
} ZChunkFile;

typedef enum {
  ZCHUNK_FILE_CREATE = 1,
  ZCHUNK_FILE_READ = 2
} ZChunkFileMode;


/* Open a zchunk file with a single process to create a new file or
   to read an existing one.
*/
ZChunkFile *ZChunkFile_open(const char *data_file, const char *index_file,
                            ZChunkFileMode mode);

/* Compress one chunk and append it to the file. */
int ZChunkFile_append(ZChunkFile *f, const void *buf, uint64_t len);

/* Read a subrange of the file.  offset and len are specified
   in terms of the original data, not the compressed data.
*/
int ZChunkFile_read_at(ZChunkFile *f, void *buf, uint64_t offset,
                       uint64_t len);

/* Close the file, deallocating memory */
void ZChunkFile_close(ZChunkFile *f);


#ifdef ZCHUNK_MPI

typedef struct {
  ZChunkIndex index;
  ZChunkEngine zip;
  MPI_File f;
  MPI_Comm comm;

  int rank, np; /* within comm */

  void *buf;  /* buffer for compressing or decompressing */
  size_t buf_size;

  void *iobuf;  /* buffer for reading */
  size_t iobuf_size;
  
  int is_creating;  /* nonzero if currently writing */

  const char *index_file_name;
  
  /* used by rank 0 to gather chunk sizes */
  uint64_t *write_offset_array;
  uint64_t write_pos;

  double time_reading, time_compressing, time_decompressing, time_hashing;
} ZChunkFileMPI;

/* Create a zchunk file and its index with multiple MPI processes.

   comm - the set of processes that will be accessing the file.  When
     ZChunkFileMPI_read_at_all() is called, every process in the
     'comm' communicator must make the call.

   alg - which compression algorithm, either ZCHUNK_ALG_GZIP or
     ZCHUNK_ALG_BZIP. Bzip tends to get a better compression ratio,
     but is slower when compressing and decompressing.

   do_hash - if nonzero, a hash of each chunk (before compression) will be
     computed and stored in the index so the data contents can be verified.
     It's a simple hash, FNV-64 (https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function)
   
   stripe_count - split the data file into this number of stripes.
     Suggested value: the nubmer of nodes used when the file is read.

   stripe_size - use this for the length of each stripe
     Suggested value: 4*1024*1024 bytes.
*/
ZChunkFileMPI *ZChunkFileMPI_create
(const char *data_file, const char *index_file,
 MPI_Comm comm, ZChunkCompressionAlgorithm alg, int do_hash,
 int stripe_count, uint64_t stripe_size);

/* Open an existing zchunk file and its index for reading with multiple
   MPI processes.

   comm - the set of processes that will be accessing the file.
          When ZChunkFileMPI_append_all() is called, every process in
          the 'comm' communicator must make the call.
*/
ZChunkFileMPI *ZChunkFileMPI_open(const char *data_file, const char *index_file,
                                  MPI_Comm comm);

/* Compress the first 'len' bytes in 'buf' and add them as a chunk to the file.
   This is a collective call, so every process in 'comm' must make the call.
   If a process does not want to add a chunk, it must set 'len' to 0.
   The chunks are stored in rank order.
   This only works when the file is opened for creating.
*/
int ZChunkFileMPI_append_all(ZChunkFileMPI *f, const void *buf, uint64_t len);

/* Returns the length of the original uncompressed data that went into
   this file. Returns 0 on error or if the file was opened for writing. */
uint64_t ZChunkFileMPI_get_length(ZChunkFileMPI *f);

/* Read one chunk of data from the file.
   This is not a collective call.
   This only works when the file is opened for reading.

   buf - buffer into which the data will be written
   offset - offset into the file of the required data
   len - length of the required data
*/
int ZChunkFileMPI_read_at(ZChunkFileMPI *f, void *buf, uint64_t offset,
                          uint64_t len);

/* All processes in 'comm' read one chunk of data from the file.
   This only works when the file is opened for reading.
*/
int ZChunkFileMPI_read_at_all(ZChunkFileMPI *f, void *buf, uint64_t offset,
                              uint64_t len);
                              
/* Close the file, deallocating memory */
void ZChunkFileMPI_close(ZChunkFileMPI *f);

#endif



#endif /* __ZCHUNK_H__ */
