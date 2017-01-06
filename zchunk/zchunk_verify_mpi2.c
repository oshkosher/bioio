/* Read a compressed file and verify the hash of each chunk. */

#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <assert.h>
#include "mpi.h"

#define ZCHUNK_MPI
#include "zchunk.h"

int rank, np;

void printHelp();
int openInputFile(const char *filename, MPI_File *f);
const char *mpiErrString(int err);
void reportTimes(double time_starting_read, double time_ending_read,
                 double time_decompressing, double time_hashing,
                 ZChunkIndex *index);
size_t longestCompressedChunk(ZChunkIndex *index);
size_t longestOriginalChunk(ZChunkIndex *index);
int readChunkStart(MPI_File f, ZChunkIndex *index, int chunk_no, void *buf);
int readChunkEnd(MPI_File f, ZChunkIndex *index, int chunk_no, void *buf);


int main(int argc, char **argv) {
  int chunk_count, chunk_pos, my_chunk, read_err;
  int bad_chunk_count = 0, active_buf = 0;
  ZChunkIndex index;
  ZChunkEngine z;
  uint64_t z_len, o_len, saved_hash, computed_hash,
    decompressed_size, unused;
  void *z_buf[2] = {0}, *o_buf = 0;
  double time_starting_read = 0, time_ending_read = 0, time_decompressing = 0,
    time_hashing = 0, start_time;
  double time0;
  MPI_File f;
  
  MPI_Init(&argc, &argv);
  MPI_Comm_size(MPI_COMM_WORLD, &np);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  if (argc != 3) printHelp();

  time0 = MPI_Wtime();
  
  /* Read the chunk index, distribute it to all ranks */
  zchunkIndexInit(&index);
  if (zchunkIndexReadColl(&index, argv[2], MPI_COMM_WORLD))
    goto fail0;
  
  zchunkEngineInit(&z, index.alg, ZCHUNK_DIR_DECOMPRESS, 0);
  
  chunk_count = zchunkIndexSize(&index);

  /* allocate buffers */
  z_len = longestCompressedChunk(&index);
  z_buf[0] = malloc(z_len);
  z_buf[1] = malloc(z_len);
  o_len = longestOriginalChunk(&index);
  o_buf = malloc(o_len);
  assert(z_buf[0] && z_buf[1] && o_buf);


  /* Open the compressed data file */
  if (openInputFile(argv[1], &f)) goto fail0;

  if (rank == 0) printf("%.3f Initialization don\n", MPI_Wtime() - time0);

  start_time = MPI_Wtime();
  readChunkStart(f, &index, rank, z_buf[0]);
  time_starting_read += MPI_Wtime() - start_time;
  
  for (chunk_pos = 0; chunk_pos < chunk_count;
       chunk_pos += np, active_buf = (active_buf ^ 1)) {
    my_chunk = chunk_pos + rank;

    /* find the compressed data for this chunk */
    if (my_chunk < chunk_count) {
      zchunkIndexGetCompressed(&index, my_chunk, &unused, &z_len, &saved_hash);
    }
    
    /* wait for my chunk to finish reading */
    start_time = MPI_Wtime();
    read_err = readChunkEnd(f, &index, my_chunk, z_buf[active_buf]);
    /* MPI_File_read_at_all(f, z_off, z_buf, z_len, MPI_BYTE, &status); */
    time_ending_read += MPI_Wtime() - start_time;

    /* start reading the next chunk */
    if (chunk_pos + np < chunk_count) {
      start_time = MPI_Wtime();
      readChunkStart(f, &index, my_chunk + np, z_buf[active_buf ^ 1]);
      time_starting_read += MPI_Wtime() - start_time;
    }
    
    if (my_chunk < chunk_count) {    

      if (read_err) {
        /* check if the read worked */
        /*
          MPI_Get_count(&status, MPI_BYTE, &bytes_read);
          if (bytes_read != z_len) {
          fprintf(stderr, "[%d] read length mismatch. At offset %" PRIu64
          ", wanted %" PRIu64 " bytes, got %d.\n",
          rank, z_off, z_len, bytes_read);
        */
              
        bad_chunk_count++;
      } else {

        /* decompress it */
        o_len = zchunkIndexGetOriginalLen(&index, my_chunk);
        start_time = MPI_Wtime();
        decompressed_size = zchunkEngineProcess
          (&z, z_buf[active_buf], z_len, o_buf, o_len);
        time_decompressing += MPI_Wtime() - start_time;
        if (decompressed_size != o_len) {
          fprintf(stderr, "[%d] chunk %d should have "
                  "decompressed to %" PRIu64 " bytes, but got %" PRIu64 "\n",
                  rank, my_chunk, o_len, decompressed_size);
          bad_chunk_count++;
        } else {

          /* hash it */
          start_time = MPI_Wtime();
          computed_hash = zchunkHash(o_buf, o_len);
          time_hashing += MPI_Wtime() - start_time;
        
          /* complain if it doesn't match */
          if (computed_hash != saved_hash) {
            fprintf(stderr, "[%d] chunk %d should have "
                    "hash of %" PRIx64 ", but got %" PRIx64 "\n",
                    rank, my_chunk, saved_hash, computed_hash);
            bad_chunk_count++;
          } else {
            /* printf("[%d] chunk %d OK\n", rank, my_chunk); */
          }
        }
      }
    }

    if (rank == 0) {
      int done = (chunk_pos + np) > chunk_count ? chunk_count : chunk_pos + np;
      printf("%.3f %d of %d chunks done\n", MPI_Wtime() - time0, done,
             chunk_count);
    }

  }

  reportTimes(time_starting_read, time_ending_read, time_decompressing,
              time_hashing, &index);

  /* Check if anyone had a bad chunk */
  MPI_Reduce(rank == 0 ? MPI_IN_PLACE : &bad_chunk_count, &bad_chunk_count, 1,
             MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);

  if (rank == 0)
    fprintf(stderr, "%d good chunks, %d bad chunks\n",
            chunk_count - bad_chunk_count, bad_chunk_count);
  
  MPI_File_close(&f);

 fail0:
  zchunkEngineClose(&z);
  zchunkIndexClose(&index);
  free(z_buf[0]);
  free(z_buf[1]);
  free(o_buf);
  MPI_Finalize();
  return 0;
}


void printHelp() {
  printf("\n  zchunk_verify_mpi <zdata> <zindex>\n\n");
  exit(1);
}

int openInputFile(const char *filename, MPI_File *f) {
  int err;
  err = MPI_File_open(MPI_COMM_WORLD, filename,
                      MPI_MODE_RDONLY | MPI_MODE_UNIQUE_OPEN,
                      MPI_INFO_NULL, f);
  if (err != MPI_SUCCESS) {
    if (rank == 0) {
      fprintf(stderr, "Failed to open input file %s: %s\n", filename,
              mpiErrString(err));
    }
    return 1;
  }
  return 0;
}


const char *mpiErrString(int err) {
  static char str[MPI_MAX_ERROR_STRING+1];
  int len;
  MPI_Error_string(err, str, &len);
  str[len] = 0;
  return str;
}



void reportTimes(double time_starting_read, double time_ending_read,
                 double time_decompressing, double time_hashing,
                 ZChunkIndex *index) {
  uint64_t original_len, compressed_len, len, offset, unused;
  int i, n = zchunkIndexSize(index);

  zchunkIndexGetCompressed(index, n-1, &len, &offset, &unused);
  compressed_len = len + offset;
  zchunkIndexGetOrig(index, n-1, &len, &offset);
  original_len = len + offset;
  
  double times[4], efftime;
  times[0] = time_starting_read;
  times[1] = time_ending_read;
  times[2] = time_decompressing;
  times[3] = time_hashing;
  MPI_Reduce(rank == 0 ? MPI_IN_PLACE : times, times, 4, MPI_DOUBLE,
             MPI_SUM, 0, MPI_COMM_WORLD);
  if (rank == 0) {
    printf("Original size %" PRIu64 ", compressed size %" PRIu64 "\n",
           original_len, compressed_len);
    for (i=0; i < 4; i++) times[i] /= np;
    printf("Starting read time %.3f sec, %.1f MB/s\n", times[0],
           compressed_len / (1024*1024*times[0]));
    printf("Ending read time %.3f sec, %.1f MB/s\n", times[1],
           compressed_len / (1024*1024*times[1]));
    printf("Decompress time %.3f sec, %.1f MB/s\n", times[2],
           original_len / (1024*1024*times[2]));
    printf("Hash time %.3f sec, %.1f MB/s\n", times[3],
           original_len / (1024*1024*times[3]));

    efftime = times[0] + times[1] + times[2];
    printf("Effective read time %.3f sec, %.1f MB/s\n", efftime,
           original_len / (1024*1024*efftime));
  }
}

uint64_t longestCompressedChunk(ZChunkIndex *index) {
  int i, n = zchunkIndexSize(index);
  uint64_t max = 0, len;

  for (i=0; i < n; i++) {
    len = zchunkIndexGetCompressedLen(index, i);
    if (len > max) max = len;
  }
  return max;
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
  

int readChunkStart(MPI_File f, ZChunkIndex *index, int chunk_no, void *buf) {
  uint64_t len, offset, unused;

  if (chunk_no >= zchunkIndexSize(index)) {
    offset = len = 0;
  } else {
    zchunkIndexGetCompressed(index, chunk_no, &offset, &len, &unused);
  }
  MPI_File_read_at_all_begin(f, offset, buf, len, MPI_BYTE);
  return 0;
}

  
int readChunkEnd(MPI_File f, ZChunkIndex *index, int chunk_no, void *buf) {
  uint64_t len;
  int bytes_read;
  MPI_Status status;

  MPI_File_read_at_all_end(f, buf, &status);

  if (chunk_no < zchunkIndexSize(index)) {
    len = zchunkIndexGetCompressedLen(index, chunk_no);
  
    MPI_Get_count(&status, MPI_BYTE, &bytes_read);
    if (bytes_read != len) {
      fprintf(stderr, "[%d] chunk %d read length mismatch. Expected %" PRIu64
              ", got %d\n", rank, chunk_no, len, bytes_read);
      return 1;
    }
  }
  
  return 0;
}
