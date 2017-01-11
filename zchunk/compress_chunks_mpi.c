#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>
#include <sys/stat.h>
#include <errno.h>
#include <ctype.h>
#include <stdint.h>
#include <inttypes.h>
#include <assert.h>
#include <pthread.h>

#include "zchunk.h"

int rank, np;
double t0;  /* start time */

#define DEFAULT_CHUNK_SIZE "4m"
#define DEFAULT_STRIPE_SIZE "4m"

typedef uint64_t u64;

typedef struct {

  ZChunkEngine zopt;

  int chunk_size;
  
  const char *infile_name;   /* input file */
  const char *outfile_name;  /* output (compressed) file */
  const char *outfile_list_name;  /* outfile file listing chunks in outfile */
  int do_compute_hash;

  int use_threads;

  int stripe_count;
  unsigned long stripe_size;

  /* other data in many places */
  MPI_Offset file_size;
  int n_iters;
  MPI_File infile;
  ZChunkFileMPI *outfile;
} Options;


typedef struct {
  void *data;
  size_t capacity, size;
  int is_full;

  pthread_mutex_t mutex;
  pthread_cond_t cv;
} Buffer;

void bufferInit(Buffer *buffer, size_t init_size);
void bufferWaitForFull(Buffer *buffer);
void bufferSignalFull(Buffer *buffer);
void bufferWaitForEmpty(Buffer *buffer);
void bufferSignalEmpty(Buffer *buffer);
void bufferClose(Buffer *buffer);

typedef struct {
  Buffer original;
  Buffer compressed;
} BufferPair;


typedef struct {
  Options *opt;
  BufferPair buffers[2];
} ThreadParams;


int parseOpt(int argc, char **argv, Options *opt);
void printHelp();
int parseSize(const char *str, uint64_t *result);
int compressFile(Options *opt);
int compressFile2(Options *opt);
MPI_Offset getFileSizeColl(MPI_File f);
const char *mpiErrString(int err);
int openInputFile(const char *infile_name, MPI_File *f, MPI_Offset *file_size);
int openOutputFile(Options *opt, MPI_File *f);
FILE* openOutputListFile(Options *opt);
void addToIndex(ZChunkIndex *chunk_index, u64 in_len, u64 out_len,
                u64 hash, u64 write_offset_array[]);
void reportTimes(double time_reading, double time_compressing,
                 double time_writing, u64 input_size, u64 output_size);


int main(int argc, char **argv) {
  Options opt;
  int err;

  MPI_Init(&argc, &argv);
  MPI_Comm_size(MPI_COMM_WORLD, &np);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  t0 = MPI_Wtime();
  
  err = parseOpt(argc, argv, &opt);
  if (err) goto fail0;

  if (openInputFile(opt.infile_name, &opt.infile, &opt.file_size))
    goto fail1;

#ifdef THREADED
  if (openOutputFile(opt, &opt.outfile))
    goto fail2;
  err = compressFileThreaded(&opt);
#else
  err = compressFile2(&opt);
#endif

#ifdef THREADED
 fail2:
  ZChunkFileMPI_close(opt.outfile);
#endif
 fail1:
  MPI_File_close(&opt.infile);
 fail0:
  MPI_Finalize();

  return err;
}

int parseOpt(int argc, char **argv, Options *opt) {
  int argno;
  const char *arg;
  uint64_t size;
  ZChunkCompressionAlgorithm alg = ZCHUNK_ALG_GZIP;
  ZChunkCompressionStrategy strat = ZCHUNK_STRATEGY_MAX_COMPRESSION;

  opt->infile_name = opt->outfile_name = opt->outfile_list_name = NULL;
  opt->do_compute_hash = 1;
  opt->stripe_count = 1;
  parseSize(DEFAULT_STRIPE_SIZE, &size);
  opt->stripe_size = size;
  parseSize(DEFAULT_CHUNK_SIZE, &size);
  opt->chunk_size = size;
  opt->use_threads = 0;

  for (argno = 1; argno < argc; argno++) {
    arg = argv[argno];
    if (arg[0] != '-') break;

    /* accept b, bzip, or bzip2 */
    if (arg[1] == 'b') {
      alg = ZCHUNK_ALG_BZIP;
    }

    /* accept g or gzip */
    else if (arg[1] == 'g') {
      alg = ZCHUNK_ALG_GZIP;
    }

    /* accept z or zstd */
    else if (arg[1] == 'z') {
      alg = ZCHUNK_ALG_FZSTD;
    }

    /* enable threads */
    else if (arg[1] == 't') {
      opt->use_threads = 1;
    }

    else if (!strcmp(arg, "-f")) {
      strat = ZCHUNK_STRATEGY_FAST;
      /*
      opt->zopt.zlib_compression_level = Z_DEFAULT_COMPRESSION;
      opt->zopt.bzlib_block_size = 3;
      */
    }

    else if (!strcmp(arg, "-a")) {
      opt->do_compute_hash = 0;
    }

    else if (!strcmp(arg, "-k")) {
      arg = argv[++argno];
      if (!arg
          || !parseSize(arg, &size)) {
        if (rank == 0) printf("Invalid chunk size\n");
        printHelp();
        return 1;
      }
      if (size > 0x7fffffff) {
        if (rank == 0) printf("Invalid chunk size, must be < 2GB\n");
      }
      opt->chunk_size = size;
    }

    else if (!strcmp(arg, "-c")) {
      arg = argv[++argno];
      if (!arg
          || 1 != sscanf(arg, "%d", &opt->stripe_count)
          || opt->stripe_count < 1) {
        if (rank == 0) printf("Invalid stripe count\n");
        printHelp();
        return 1;
      }
    }        
    
    else if (!strcmp(arg, "-s")) {
      arg = argv[++argno];
      if (!arg || !parseSize(arg, &size)) {
        if (rank == 0) printf("Invalid stripe size\n");
        printHelp();
        return 1;
      }
      opt->stripe_size = size;
    }

    else {
      if (rank == 0) {
        printf("Invalid argument: \"%s\"\n", arg);
        printHelp();
      }
      return 1;
    }
  }

  if (argc - argno != 3) {
    if (rank == 0) printf("Expected 3 filenames\n");
    printHelp();
    return 1;
  }

  zchunkEngineInit(&opt->zopt, alg, ZCHUNK_DIR_COMPRESS, strat);
  opt->infile_name = argv[argno++];
  opt->outfile_name = argv[argno++];
  opt->outfile_list_name = argv[argno++];

  return 0;
}
  
void printHelp() {
  if (rank != 0) return;

  printf(
"compress_chunks_mpi [options] <infile> <outfile> <outfile_list>\n"
"  Read infile, split it into equal-sized chunks, compress each\n"
"  chunk, write the chunks to outfile, and write the list of chunks\n"
"  to outfile_list. Each chunk many be decompressed separately,\n"
"  or the whole file many be decompressed as one with gunzip or bunzip2.\n"
"  options:\n");

  printf(
"   -g : use gzip to compress (default)\n"
"   -b : use bzip2 to compress\n"
"   -z : use Facebook ZSTD to compress\n"
"   -f : use faster compression\n"
"   -a : disable chunk hash computation\n"
"   -k <size> : set chunk size (%s by default) may use k, m, g suffixes\n"
"               must be less than 2 GiB\n"
"   -c <count> : set striping count of result (1 by default)\n"
"   -s <size> : set stripe size (%s by default)\n"
"   -t : use threads to overlap compute and I/O\n"
"  <size> arguments are bytes. 'k', 'm', and 'g' suffixes are supported.\n"
"\n",
  DEFAULT_CHUNK_SIZE, DEFAULT_STRIPE_SIZE);
}


#if 0
int compressFile(Options *opt) {
  MPI_Offset file_size, read_pos, write_pos = 0;
  uint64_t *write_offset_array = NULL;
  MPI_File outfile, infile;
  int err, any_bad_chunks = 0;
  size_t output_buf_len;
  void *input_buf = NULL, *output_buf = NULL;
  double time_reading = 0, time_compressing = 0, time_writing = 0;
  BufferPair buf;

  bufferInit(&buf.original, opt->chunk_size);
  bufferInit(&buf.compressed,
             zchunkMaxCompressedSize(opt->zopt.alg, opt->chunk_size));

  err = 0;
  if (rank == 0) {
    write_offset_array = (uint64_t*) malloc(sizeof(uint64_t) * np * 4);
    zchunkIndexInit(&chunk_index);
    chunk_index.alg = opt->zopt.alg;
    chunk_index.has_hash = opt->do_compute_hash;
    err = !write_offset_array;
  }

  /* allocate input and output buffers */
  input_buf = malloc(opt->chunk_size);
  if (!input_buf) {
    fprintf(stderr, "[%d] failed to allocate input buffer\n", rank);
    err = 1;
  }

  output_buf_len = zchunkMaxCompressedSize(opt->zopt.alg, opt->chunk_size);
  output_buf = malloc(output_buf_len);
  if (!output_buf) {
    fprintf(stderr, "[%d] failed to allocate output buffer\n", rank);
    err = 1;
  }
  

  /* check if anyone failed */
  MPI_Allreduce(MPI_IN_PLACE, &err, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);
  if (err) goto fail0;

  if (rank==0)
    printf("%.3f Initialization done\n", MPI_Wtime() - t0);

  /* Start reading a chunk into buffer1-original */

  /* Process np chunks at a time, collectively */
  for (read_pos = 0; read_pos < file_size;
       read_pos += opt->chunk_size * np) {
    MPI_Offset read_offset;
    int read_len, read_len_result;
    int write_len_result;
    MPI_Status status;
    uint64_t cumulative_len, output_len, hash;
    double start_time;
      
    /* Set my offset and len, making sure not to read past the end of
       the file */
    read_offset = read_pos + rank * opt->chunk_size;
    if (read_offset >= file_size) {
      read_offset = 0; /* or would (file_size - 1) be better? */
      read_len = 0;
    } else {
      read_len = opt->chunk_size;
      if (read_offset + read_len > file_size)
        read_len = file_size - read_offset;
    }

    output_len = 0;
    hash = 0;

    /* read a chunk */
    start_time = MPI_Wtime();
    MPI_File_read_at_all(infile, read_offset, input_buf, read_len,
                         MPI_BYTE, &status);
    time_reading += MPI_Wtime() - start_time;
    MPI_Get_count(&status, MPI_BYTE, &read_len_result);
    if (read_len_result != read_len) {
      fprintf(stderr, "[%d] read length mismatch. At offset %" PRIu64
              ", file length %" PRIu64 ", wanted %d bytes, got %d.\n",
              rank, (u64)read_offset, (u64)file_size,
              read_len, read_len_result);
      any_bad_chunks = 1;
    } else {

      /* wait for the chunk to finish reading */
      /* start the next one reading */
      
      if (read_len > 0) {
        if (opt->do_compute_hash)
          hash = zchunkHash(input_buf, read_len);
      
        /* compress the chunk */
        start_time = MPI_Wtime();
        output_len = zchunkEngineProcess(&opt->zopt, input_buf, read_len,
                                         output_buf, output_buf_len);
                                  
        time_compressing += MPI_Wtime() - start_time;
        if (err) {
          output_len = 0;
          any_bad_chunks = 1;
        }
      }
    }

    /* figure out where to write my data */
    cumulative_len = output_len;
    MPI_Scan(MPI_IN_PLACE, &cumulative_len, 1, MPI_UINT64_T, MPI_SUM,
             MPI_COMM_WORLD);
    write_pos += cumulative_len - output_len;
    /*
    fprintf(stderr, "[%d] write %" PRIu64 " bytes at %" PRIu64 "\n", rank,
            output_len, (uint64_t) write_pos);
    */

    /* write the chunk */
    start_time = MPI_Wtime();
    MPI_File_write_at_all(outfile, write_pos, output_buf, output_len, MPI_BYTE,
                          &status);
    time_writing += MPI_Wtime() - start_time;
    MPI_Get_count(&status, MPI_BYTE, &write_len_result);
    if (write_len_result != output_len) {
      fprintf(stderr, "[%d] write failure: only %d of "
              "%" PRIu64 " bytes written\n",
              rank, write_len_result, output_len);
    }

    /* every rank sends destination and length to the root process, which will
       write them to outfile_list */
    addToIndex(&chunk_index, read_len, output_len, hash, write_offset_array);

    write_pos += output_len;

    /* The last rank knows where its data ended, so have it tell everyone
       where next chunks should go. */
    MPI_Bcast(&write_pos, 1, MPI_UINT64_T, np-1, MPI_COMM_WORLD);

    /* status report */
    if (rank == 0) {
      u64 total_bytes_read = read_pos + opt->chunk_size * np;
      if (total_bytes_read > file_size)
        total_bytes_read = file_size;
      printf("%.3f %.2f%% done, %" PRIu64 " bytes read, %" PRIu64
              " bytes written\n",
              MPI_Wtime() - t0,
              100.0 * total_bytes_read / file_size,
              total_bytes_read, (u64) write_pos);
    }
  }

  if (rank == 0) {
    zchunkIndexWrite(&chunk_index, opt->outfile_list_name);
    zchunkIndexClose(&chunk_index);
  }

  reportTimes(time_reading, time_compressing, time_writing, file_size,
              write_pos);
  
  
 fail2:
  MPI_File_close(&outfile);
 fail1:
  MPI_File_close(&infile);
 fail0:
  free(input_buf);
  free(output_buf);
  free(write_offset_array);

  /* Check if anyone had a bad chunk */
  MPI_Reduce(rank == 0 ? MPI_IN_PLACE : &any_bad_chunks, &any_bad_chunks, 1,
             MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);

  if (rank == 0 && any_bad_chunks)
    fprintf(stderr, "ERROR: errors encountered with some chunks\n");
  
  return err;
}
#endif


int compressFile2(Options *opt) {
  ZChunkFileMPI *outfile;
  MPI_Offset read_pos;
  int any_bad_chunks = 0;
  /* size_t output_buf_len; */
  /* void *input_buf = NULL, *output_buf = NULL; */
  double time_reading = 0, time_compressing = 0, time_writing = 0;
  /* BufferPair buf; */
  Buffer buf;

  printf("[%d] Creating chunkfile\n", rank);
  outfile = ZChunkFileMPI_create
    (opt->outfile_name, opt->outfile_list_name, MPI_COMM_WORLD, opt->zopt.alg,
     opt->do_compute_hash, opt->stripe_count, opt->stripe_size);
     
  if (!outfile) {
    printf("[%d] failed to open output file\n", rank);
    return 1;
  }

  bufferInit(&buf, opt->chunk_size);

  if (rank==0)
    printf("%.3f Initialization done\n", MPI_Wtime() - t0);

  /* Start reading a chunk into buffer1-original */

  /* Process np chunks at a time, collectively */
  for (read_pos = 0; read_pos < opt->file_size;
       read_pos += opt->chunk_size * np) {
    MPI_Offset read_offset;
    int read_len, read_len_result;
    MPI_Status status;
    double start_time;
      
    /* Set my offset and len, making sure not to read past the end of
       the file */
    read_offset = read_pos + rank * opt->chunk_size;
    if (read_offset >= opt->file_size) {
      read_offset = 0; /* or would (file_size - 1) be better? */
      read_len = 0;
    } else {
      read_len = opt->chunk_size;
      if (read_offset + read_len > opt->file_size)
        read_len = opt->file_size - read_offset;
    }

    /* read a chunk */
    start_time = MPI_Wtime();
    MPI_File_read_at_all(opt->infile, read_offset, buf.data, read_len,
                         MPI_BYTE, &status);
    time_reading += MPI_Wtime() - start_time;
    MPI_Get_count(&status, MPI_BYTE, &read_len_result);
    if (read_len_result != read_len) {
      fprintf(stderr, "[%d] read length mismatch. At offset %" PRIu64
              ", file length %" PRIu64 ", wanted %d bytes, got %d.\n",
              rank, (u64)read_offset, (u64)opt->file_size,
              read_len, read_len_result);
      any_bad_chunks = 1;
    } else {

      /* write the chunk */
      printf("[%d] chunk at %" PRIu64 " read\n", rank, (u64)read_offset);
      ZChunkFileMPI_append_all(outfile, buf.data, read_len);
      printf("[%d] chunk at %" PRIu64 " saved\n", rank, (u64)read_offset);
      

    }

    /* status report */
    if (rank == 0) {
      u64 total_bytes_read = read_pos + opt->chunk_size * np;
      if (total_bytes_read > opt->file_size)
        total_bytes_read = opt->file_size;
      printf("%.3f %.2f%% done, %" PRIu64 " bytes read, %" PRIu64
              " bytes written\n",
              MPI_Wtime() - t0,
              100.0 * total_bytes_read / opt->file_size,
              total_bytes_read, (u64) outfile->write_pos);
    }
  }

  time_compressing = time_writing = 0;
  reportTimes(time_reading, time_compressing, time_writing, opt->file_size,
              outfile->write_pos);
  
  bufferClose(&buf);
  ZChunkFileMPI_close(outfile);

  /* Check if anyone had a bad chunk */
  MPI_Reduce(rank == 0 ? MPI_IN_PLACE : &any_bad_chunks, &any_bad_chunks, 1,
             MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);

  if (rank == 0 && any_bad_chunks) {
    fprintf(stderr, "ERROR: errors encountered with some chunks\n");
    return 1;
  } else {
    return 0;
  }
}


#if 0
int compressFileThreaded(Options *opt, MPI_File infile, u64 file_size,
                         ZChunkFileMPI *outfile) {
  u64 max_zlen;
  ZChunkFileMPI *zfile;
  ThreadParams tp;
  pthread_t read_thread, compress_thread, write_thread;
  
  /* allocate input and output buffers */
  max_zlen = zchunkMaxCompressedSize(opt->zopt.alg, opt->chunk_size);
  bufferInit(&tp.buffers[0].original, opt->chunk_size);
  bufferInit(&tp.buffers[1].original, opt->chunk_size);
  bufferInit(&tp.buffers[0].compressed, max_zlen);
  bufferInit(&tp.buffers[1].compressed, max_zlen);

  /*
  zfile = ZChunkFileMPI_create
    (opt->outfile_name, opt->outfile_list_name, MPI_COMM_WORLD, opt->zopt.alg,
     opt->do_compute_hash);
  */
  
  if (rank==0)
    printf("%.3f Initialization done. Starting threads.\n", MPI_Wtime() - t0);

  threadParams.opt = opt;
  threadParams.n_iters = (file_size + opt->chunk_size * np - 1)
    / (opt->chunk_size * np);
  threadParams.file_size = file_size;
  threadParams.infile = infile;
  threadParams.outfile = outfile;

  /* start threads */
  pthread_create(&read_thread, NULL, readThreadFn, &tp);
  pthread_create(&compress_thread, NULL, compressThreadFn, &tp);
  pthread_create(&write_thread, NULL, writeThreadFn, &tp);


  /* wait for threads to finish */
  pthread_join(read_thread, NULL);
  pthread_join(compress_thread, NULL);
  pthread_join(write_thread, NULL);

  printf("[%d] all threads done\n", rank);

  /*
  ZChunkFileMPI_close(zfile);
  */
    
  /* deallocate buffers */
  bufferClose(&tp.buffers[0].original);
  bufferClose(&tp.buffers[1].original);
  bufferClose(&tp.buffers[0].compressed);
  bufferClose(&tp.buffers[1].compressed);
}
#endif

int openInputFile(const char *infile_name, MPI_File *f, MPI_Offset *file_size) {
  int err;

  err = MPI_File_open(MPI_COMM_WORLD, infile_name,
                      MPI_MODE_RDONLY | MPI_MODE_UNIQUE_OPEN,
                      MPI_INFO_NULL, f);
  if (err != MPI_SUCCESS) {
    if (rank == 0) {
      fprintf(stderr, "Failed to open input file %s: %s\n", infile_name,
              mpiErrString(err));
    }
    return 1;
  }

  *file_size = getFileSizeColl(*f);

  if (*file_size == 0) {
    if (rank == 0)
      fprintf(stderr, "Error: failed to get size of %s, or size is 0\n",
              infile_name);
    MPI_File_close(f);
    return 1;
  }

  return 0;
}


void bufferInit(Buffer *buffer, size_t init_size) {
  buffer->data = malloc(init_size);
  assert(buffer->data);
  buffer->capacity = init_size;
  buffer->size = 0;
  buffer->is_full = 0;

  pthread_mutex_init(&buffer->mutex, NULL);
  pthread_cond_init(&buffer->cv, NULL);
}
  
void bufferWaitForFull(Buffer *buffer) {
  pthread_mutex_lock(&buffer->mutex);
  while (!buffer->is_full)
    pthread_cond_wait(&buffer->cv, &buffer->mutex);
  pthread_mutex_unlock(&buffer->mutex);
}

void bufferSignalFull(Buffer *buffer) {
  pthread_mutex_lock(&buffer->mutex);
  buffer->is_full = 1;
  pthread_cond_broadcast(&buffer->cv);
  pthread_mutex_unlock(&buffer->mutex);
}

void bufferWaitForEmpty(Buffer *buffer) {
  pthread_mutex_lock(&buffer->mutex);
  while (buffer->is_full)
    pthread_cond_wait(&buffer->cv, &buffer->mutex);
  pthread_mutex_unlock(&buffer->mutex);
}

void bufferSignalEmpty(Buffer *buffer) {
  pthread_mutex_lock(&buffer->mutex);
  buffer->is_full = 0;
  pthread_cond_broadcast(&buffer->cv);
  pthread_mutex_unlock(&buffer->mutex);
}


void bufferClose(Buffer *buffer) {
  pthread_mutex_destroy(&buffer->mutex);
  pthread_cond_destroy(&buffer->cv);
}

#if 0
void* readThreadFn(void *param) {
  ThreadParams *p = (ThreadParams*) param;
  int chunk_no, iter_no;
  int buf_no = 0;

  for (chunk_no = rank, iter_no = 0;
       iter_no < p->n_iters;
       chunk_no += np, buf_no ^= 1) {
    Buffer *b = &p->buffers[buf_no].original;
    bufferWaitForEmpty(b);
    printf("[%d] reading chunk %d into buffer %d\n", rank, chunk_no, buf_no);
    readChunk(chunk_no, b);
    bufferSignalFull(b);
  }
}


void* compressThreadFn(void *param) {
  ThreadParams *p = (ThreadParams*) param;
  int chunk_no = rank;
  int buf_no = 0;

  for (chunk_no = rank, iter_no = 0;
       iter_no < p->n_iters;
       chunk_no += np, buf_no ^= 1) {
    BufferPair *bp = &buffers[buf_no];
    bufferWaitForFull(&bp->original);
    bufferWaitForEmpty(&bp->compressed);
    printf("[%d] compress chunk %d in buffer %d\n", rank, chunk_no, buf_no);
    compressChunk(&bp->original, &bp->compressed);
    bufferSignalEmpty(&bp->original);
    bufferSignalFull(&bp->compressed);
  }
}


void* writerThreadFn(void *param) {
  ThreadParams *p = (ThreadParams*) param;
  int chunk_no = rank;
  int buf_no = 0;

  for (chunk_no = rank, iter_no = 0;
       iter_no < p->n_iters;
       chunk_no += np, buf_no ^= 1) {
    Buffer *b = &buffers[buf_no].compressed;
    bufferWaitForFull(b);
    printf("[%d] write chunk %d in buffer %d\n", rank, chunk_no, buf_no);
    writeChunk(b);
    bufferSignalEmpty(b);
  }
}
#endif



int openOutputFile(Options *opt, MPI_File *f) {
  MPI_Info info;
  char stripe_count_str[50], stripe_size_str[50];
  int err;
  
  sprintf(stripe_count_str, "%d", opt->stripe_count);
  sprintf(stripe_size_str, "%lu", opt->stripe_size);

  MPI_Info_create(&info);
  MPI_Info_set(info, "striping_factor", stripe_count_str);
  MPI_Info_set(info, "striping_unit", stripe_size_str);

  /* delete the file first, because we can't set the striping if it exists */
  if (rank == 0)
    MPI_File_delete(opt->outfile_name, MPI_INFO_NULL);

  /* wait for rank 0 to delete the file */
  MPI_Barrier(MPI_COMM_WORLD);
  
  err = MPI_File_open(MPI_COMM_WORLD, opt->outfile_name,
                      MPI_MODE_WRONLY | MPI_MODE_UNIQUE_OPEN | MPI_MODE_CREATE,
                      info, f);
  if (err != MPI_SUCCESS) {
    if (rank == 0) {
      fprintf(stderr, "Failed to open output file %s: %s\n", opt->outfile_name,
              mpiErrString(err));
    }
    return 1;
  }
  MPI_Info_free(&info);

  /* Truncate the output file--we'll completely rewrite it */
  MPI_File_set_size(*f, 0);

  return 0;
}
  

/* Write each rank's data to outfile_list */
void addToIndex(ZChunkIndex *chunk_index, u64 in_len, u64 out_len,
                u64 hash, u64 write_offset_array[]) {
                
  u64 data[3];
  int i;

  /* rank 0 gathers per-chunk data from all the other ranks */
  data[0] = in_len;
  data[1] = out_len;
  data[2] = hash;
  MPI_Gather(data, 3, MPI_UINT64_T, write_offset_array, 3, MPI_UINT64_T,
             0, MPI_COMM_WORLD);

  /* and rank 0 writes the data to the list file */
  if (rank == 0) {
    for (i=0; i < np; i++) {
      u64 *p = write_offset_array + (i * 3);

      /* skip if the length is 0 */
      if (p[1] != 0) {
        zchunkIndexAdd(chunk_index, p[0], p[1], p[2]);
      }
    }
  }
}


void reportTimes(double time_reading, double time_compressing,
                 double time_writing, u64 input_size, u64 output_size) {
  double times[3];
  times[0] = time_reading;
  times[1] = time_compressing;
  times[2] = time_writing;
  MPI_Reduce(rank == 0 ? MPI_IN_PLACE : times, times, 3, MPI_DOUBLE,
             MPI_SUM, 0, MPI_COMM_WORLD);
  if (rank == 0) {
    times[0] /= np;
    times[1] /= np;
    times[2] /= np;
    printf("Read time %.3f sec, %.1f MB/s\n", times[0],
           input_size / (1024*1024*times[0]));
    printf("Compress time %.3f sec, %.1f MB/s\n", times[1],
           input_size / (1024*1024*times[1]));
    printf("Write time %.3f sec, %.1f MB/s\n", times[2],
           output_size / (1024*1024*times[2]));
  }
}



MPI_Offset getFileSizeColl(MPI_File f) {
  MPI_Offset size;

  if (rank == 0)
    MPI_File_get_size(f, &size);

  MPI_Bcast(&size, 1, MPI_OFFSET, 0, MPI_COMM_WORLD);
  return size;
}


MPI_Offset getFileSizeCollByName(const char *filename) {
  MPI_Offset size;

  /* Or:
     int MPI_File_get_size(MPI_File fh, MPI_Offset *size) */

  if (rank == 0) {
    struct stat stats;
    if (stat(filename, &stats)) {
      fprintf(stderr, "Error getting size of \"%s\": %s\n",
              filename, strerror(errno));
      size = 0;
    } else {
      size = stats.st_size;
    }
  }

  MPI_Bcast(&size, 1, MPI_OFFSET, 0, MPI_COMM_WORLD);
  return size;
}


/* Parse a number with a case-insensitive magnitude suffix:
     k : multiply by 1024
     m : multiply by 1024*1024
     g : multiply by 1024*1024*1024

   For example, "32m" would parse as 33554432.

   Returns 1 on success, 0 on failure.
*/
int parseSize(const char *str, uint64_t *result) {
  uint64_t multiplier = 1;
  const char *last;
  char suffix;
  
  /* missing argument check */
  if (!str || !str[0]) return 0;

  last = str + strlen(str) - 1;
  suffix = tolower((int)*last);
  if (suffix == 'k')
    multiplier = 1024;
  else if (suffix == 'm')
    multiplier = 1024*1024;
  else if (suffix == 'g')
    multiplier = 1024*1024*1024;
  else if (!isdigit((int)suffix))
    return 0;

  /* if (multiplier > 1) */
  if (!sscanf(str, "%" SCNu64, result))
    return 0;

  *result *= multiplier;
  return 1;
}


const char *mpiErrString(int err) {
  static char str[MPI_MAX_ERROR_STRING+1];
  int len;
  MPI_Error_string(err, str, &len);
  str[len] = 0;
  return str;
}

        
