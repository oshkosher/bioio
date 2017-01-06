/* Read a compressed file and verify the hash of each chunk. */

#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <assert.h>
#include "mpi.h"

#define ZCHUNK_MPI
#include "zchunk.h"

#define COLLECTIVE_READ 1

int rank, np, chunk_count, bad_chunk_count = 0;
double time0, time_hashing = 0;

void printHelp();
void reportTimes(double time_reading, double time_decompressing,
                 double time_hashing, ZChunkIndex *index);
size_t longestOriginalChunk(ZChunkIndex *index);
int openOriginalFile(const char *filename, MPI_File *f, uint64_t *length);

void compareChunkContent(ZChunkFileMPI *f, MPI_File orig_file);
void compareChunkHashes(ZChunkFileMPI *f);
void compareFixedLenContent(ZChunkFileMPI *f, MPI_File orig_file,
                            int compare_len);



int main(int argc, char **argv) {
  int use_chunk_list = 1;
  uint64_t orig_len, compare_len;
  ZChunkFileMPI *f;
  const char *orig_filename = NULL;
  MPI_File orig_file = MPI_FILE_NULL;

  MPI_Init(&argc, &argv);
  MPI_Comm_size(MPI_COMM_WORLD, &np);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  if (argc < 3) {
    printHelp();
  } else if (argc > 3) {
    orig_filename = argv[3];
    if (argc > 4) {
      if (1 != sscanf(argv[4], "%" SCNu64, &compare_len)) {
        if (rank == 0) {
          printf("Invalid chunk length '%s'\n", argv[4]);
          return 1;
        } else {
          use_chunk_list = 0;
        }
      }
      if (argc > 5) printHelp();
    }
  }
        
  time0 = MPI_Wtime();

  /* open the compressed file */
  printf("[%d] opening %s/%s\n", rank, argv[1], argv[2]);
  f = ZChunkFileMPI_open(argv[1], argv[2], MPI_COMM_WORLD);
  if (!f) {
    printf("[%d] Failed to open %s\n", rank, argv[1]);
    goto fail0;
  }

  orig_len = ZChunkFileMPI_get_length(f);
  
  /* open the original file */
  if (orig_filename) {
    uint64_t compare_file_len;
    if (openOriginalFile(orig_filename, &orig_file, &compare_file_len)) {
      goto fail1;
    }
    if (orig_len != compare_file_len) {
      if (rank == 0)
        printf("ERROR: original file length %" PRIu64 ", %s length %" PRIu64
               "\n", orig_len, orig_filename, compare_file_len);
      goto fail2;
    }
  }

  /* allocate buffer */
  /*
  if (use_chunk_list) {
    o_len = longestOriginalChunk(&f->index);
  } else {
    o_len = chunk_len;
  }
  o_buf = malloc(o_len);
  assert(o_buf);
  */

  if (rank == 0) printf("%.3f Initialization done\n", MPI_Wtime() - time0);

  if (use_chunk_list) {
    if (orig_filename) {
      compareChunkContent(f, orig_file);
    } else {
      compareChunkHashes(f);
    }
    chunk_count = zchunkIndexSize(&f->index);
  } else {
    compareFixedLenContent(f, orig_file, compare_len);
    chunk_count = (ZChunkFileMPI_get_length(f) + compare_len - 1) / compare_len;
  }

  reportTimes(f->time_reading, f->time_decompressing,
              time_hashing, &f->index);

  /* Check if anyone had a bad chunk */
  MPI_Reduce(rank == 0 ? MPI_IN_PLACE : &bad_chunk_count, &bad_chunk_count, 1,
             MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);

  if (rank == 0)
    fprintf(stderr, "%d good chunks, %d bad chunks\n",
            chunk_count - bad_chunk_count, bad_chunk_count);

 fail2:
  if (orig_file != MPI_FILE_NULL)
    MPI_File_close(&orig_file);
 fail1:
  ZChunkFileMPI_close(f);
 fail0:
  MPI_Finalize();
  return 0;
}


void printHelp() {
  printf("\n  zchunk_verify_mpi <zdata> <zindex> [original_file [chunk_len]]\n"
         "  If original_file is specified, hashes will be compared with that\n"
         "  rather than the saved hashes.\n"
         "  If chunk_len is specified, the original chunks will be ignored,\n"
         "  and chunks of that length will be used instead.\n"
         );
  exit(1);
}


void reportTimes(double time_reading, double time_decompressing,
                 double time_hashing, ZChunkIndex *index) {
  uint64_t original_len, compressed_len, len, offset, unused;
  int i, n = zchunkIndexSize(index);

  zchunkIndexGetCompressed(index, n-1, &len, &offset, &unused);
  compressed_len = len + offset;
  zchunkIndexGetOrig(index, n-1, &len, &offset);
  original_len = len + offset;
  
  double times[3], efftime;
  times[0] = time_reading;
  times[1] = time_decompressing;
  times[2] = time_hashing;
  MPI_Reduce(rank == 0 ? MPI_IN_PLACE : times, times, 3, MPI_DOUBLE,
             MPI_SUM, 0, MPI_COMM_WORLD);
  if (rank == 0) {
    printf("Original size %" PRIu64 ", compressed size %" PRIu64 "\n",
           original_len, compressed_len);
    for (i=0; i < 3; i++) times[i] /= np;
    printf("Read time %.3f sec, %.1f MB/s\n", times[0],
           compressed_len / (1024*1024*times[0]));
    printf("Decompress time %.3f sec, %.1f MB/s\n", times[1],
           original_len / (1024*1024*times[1]));
    printf("Hash time %.3f sec, %.1f MB/s\n", times[2],
           original_len / (1024*1024*times[2]));

    efftime = times[0] + times[1];
    printf("Effective read time %.3f sec, %.1f MB/s\n", efftime,
           original_len / (1024*1024*efftime));
  }
}


uint64_t longestOriginalChunk(ZChunkIndex *index) {
  int i, n = zchunkIndexSize(index);
  uint64_t max = 0, len;

  for (i=0; i < n; i++) {
    len = zchunkIndexGetOriginalLen(index, i);
    if (len > max) max = len;
  }
  return max;
}

int openOriginalFile(const char *filename, MPI_File *f, uint64_t *length) {
  int err;
  MPI_Offset len_offset;

  err = MPI_File_open(MPI_COMM_WORLD, filename,
                      MPI_MODE_RDONLY | MPI_MODE_UNIQUE_OPEN,
                      MPI_INFO_NULL, f);
  if (err != MPI_SUCCESS) {
    if (rank == 0) {
      fprintf(stderr, "Failed to open input file %s, error %d\n",
              filename, err);
    }
    return 1;
  }

  if (rank == 0) {
    MPI_File_get_size(*f, &len_offset);
  }

  MPI_Bcast(&len_offset, 1, MPI_OFFSET, 0, MPI_COMM_WORLD);
  *length = len_offset;
  printf("rank %d got length %" PRIu64 "\n", rank, *length);
  return 0;
}

void compareChunkContent(ZChunkFileMPI *f, MPI_File orig_file) {
  int chunk_pos, my_chunk, longest, bytes_read;
  uint64_t chunk_offset, chunk_len, hash_restored, hash_orig;
  void *o_buf, *c_buf;
  double start_time;
  MPI_Status status;

  longest = longestOriginalChunk(&f->index);
  o_buf = malloc(longest);
  c_buf = malloc(longest);
  assert(o_buf && c_buf);
  
  chunk_count = zchunkIndexSize(&f->index);
  for (chunk_pos = 0; chunk_pos < chunk_count; chunk_pos += np) {
       
    my_chunk = chunk_pos + rank;

    if (my_chunk < chunk_count) {

      /* look up the range of data for this chunk */
      zchunkIndexGetOrig(&f->index, my_chunk, &chunk_offset, &chunk_len);

      /* read and decompress the compressed chunk */
#if COLLECTIVE_READ
      ZChunkFileMPI_read_at_all
#else
        ZChunkFileMPI_read_at
#endif
        (f, o_buf, chunk_offset, chunk_len);

      /* read the uncompressed chunk */
#if COLLECTIVE_READ
      MPI_File_read_at_all
#else
      MPI_File_read_at
#endif
        (orig_file, chunk_offset, c_buf, chunk_len, MPI_BYTE, &status);
         
      MPI_Get_count(&status, MPI_BYTE, &bytes_read);
      if (bytes_read != chunk_len) {
        printf("[%d] failed to read %" PRIu64 " bytes at %" PRIu64
               ", read %d bytes instead\n", rank, chunk_len, chunk_offset,
               bytes_read);
        bad_chunk_count++;
      } else {
      
        /* hash the result */
        start_time = MPI_Wtime();
        hash_restored = zchunkHash(o_buf, chunk_len);
        hash_orig = zchunkHash(c_buf, chunk_len);
        time_hashing += MPI_Wtime() - start_time;

        if (hash_restored != hash_orig) {
          printf("[%d] chunk %d mismatch, expected %" PRIx64 ", got %" PRIx64
                 "\n", rank, my_chunk, hash_orig, hash_restored);
          bad_chunk_count++;
        }
      }
    }

    if (rank == 0) {
      int done = (chunk_pos + np) > chunk_count ? chunk_count : chunk_pos + np;
      printf("%.3f %d of %d chunks done\n", MPI_Wtime() - time0, done,
             chunk_count);
    }
  }

}


void compareChunkHashes(ZChunkFileMPI *f) {
  int chunk_pos, my_chunk;
  uint64_t chunk_offset, chunk_len, hash_saved, hash_computed;
  void *o_buf;
  double start_time;

  o_buf = malloc(longestOriginalChunk(&f->index));
  assert(o_buf);
  
  chunk_count = zchunkIndexSize(&f->index);
  for (chunk_pos = 0; chunk_pos < chunk_count; chunk_pos += np) {
       
    my_chunk = chunk_pos + rank;

    if (my_chunk < chunk_count) {

      /* look up the range of data for this chunk */
      zchunkIndexGetOrig(&f->index, my_chunk, &chunk_offset, &chunk_len);
      hash_saved = zchunkIndexGetHash(&f->index, my_chunk);

      /* read the chunk */
#if COLLECTIVE_READ
      ZChunkFileMPI_read_at_all
#else
        ZChunkFileMPI_read_at
#endif
        (f, o_buf, chunk_offset, chunk_len);
      
      /* hash the result */
      start_time = MPI_Wtime();
      hash_computed = zchunkHash(o_buf, chunk_len);
      time_hashing += MPI_Wtime() - start_time;

      if (hash_computed != hash_saved) {
        printf("[%d] chunk %d mismatch, expected %" PRIx64 ", got %" PRIx64
               "\n", rank, my_chunk, hash_saved, hash_computed);
        bad_chunk_count++;
      }
    }

    if (rank == 0) {
      int done = (chunk_pos + np) > chunk_count ? chunk_count : chunk_pos + np;
      printf("%.3f %d of %d chunks done\n", MPI_Wtime() - time0, done,
             chunk_count);
    }
  }

}


void compareFixedLenContent(ZChunkFileMPI *f, MPI_File orig_file,
                            int compare_len) {
}
