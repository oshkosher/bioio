/*
read
4, 848.601218
8, 1717.666105
16, 3234.442613
32, 6553.155885
64, 13351.654266
128, 25970.713274
256, 51219.137046
512, 99320.663336
1024, 205518.461124
2048, 393990.871462
4096, 768773.878723
8192, 1560940.307056
16384, 3150836.658002
32768, 5941387.911676
65536, 10846285.924201
131072, 19209844.826065
262144, 33870830.758500
524288, 56036052.480948
1048576, 82513882.248122
2097152, 115439791.119491
4194304, 138681747.662453
8388608, 176253974.181147
16777216, 205070391.907980
*/

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>
#include <assert.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include "common.h"

typedef uint64_t u64;

int min_test_len = 4;
int max_test_len = 16 * 1024 * 1024;
double target_test_sec = 3.0;

void printHelp();
void computeReadSpeed(const char *filename, u64 file_size);
void computeWriteSpeed(const char *filename, u64 file_size);
void computeReadSpeed2(const char *filename, u64 file_size);
void computeWriteSpeed2(const char *filename, u64 file_size);
u64 myrand(u64 max);

int main(int argc, char **argv) {
  const char *filename = argv[1];
  u64 file_size;
  
  srand48((long)time(NULL));

  if (argc != 2) printHelp();

  file_size = getFileSize(filename);
  if (file_size == 0) return 1;

  /* computeReadSpeed2(filename, file_size); */
  computeWriteSpeed2(filename, file_size);
  
  return 0;
}


void printHelp() {
  printf("\n  disk_speed <filename>\n\n"
         "  Compute read and write speed of the given file.\n"
         "  !! The file's contents will be corrupted during the write test !!\n"
         "\n");
  exit(1);
}


/* return a random number in [0..max) */
u64 myrand(u64 max) {
  u64 r = lrand48();
  r = r ^ ((u64)lrand48() <<32);
  return r % max;
}


void computeReadSpeed(const char *filename, u64 file_size) {
  int len, n_tests, test_iter, read_len;
  char *buf;
  double test_time = 0, bytes_per_sec = 0;
  FILE *f = fopen(filename, "r");
  assert(f);

  buf = (char*) malloc(max_test_len);
  assert(buf);

  printf("read\n");

  /* run tests for varying read lengths */
  for (len = min_test_len; len <= max_test_len; len *= 2) {

    /* do enough iterations that it takes at least (target_test_sec) seconds
       to do them all */
    test_time = 0;
    for (n_tests = 1; n_tests < 1000000 && test_time < target_test_sec;
         n_tests *= 2) {
      
      double start_time = getSeconds();
      for (test_iter = 0; test_iter < n_tests; test_iter++) {
        u64 position = myrand(file_size - len);
        fseek(f, position, SEEK_SET);
        read_len = fread(buf, len, 1, f);
        if (read_len != 1) {
          fprintf(stderr, "fread of %d bytes at %" PRIu64 " failed\n",
                  len, position);
          return;
        }
      }
      test_time = getSeconds() - start_time;

      bytes_per_sec = (u64)len * n_tests / test_time;
      /*
      printf("len %d, %d iters, %.3f sec, %.3f MiB/s\n",
             len, n_tests, test_time, bytes_per_sec / (1024 * 1024));
      */
    }
    
    printf("%d, %f\n", len, bytes_per_sec);
  }
  free(buf);
  fclose(f);
}


void computeWriteSpeed(const char *filename, u64 file_size) {
  int len, n_tests, test_iter, write_len;
  char *buf;
  double test_time = 0, bytes_per_sec = 0;
  FILE *f = fopen(filename, "w");
  assert(f);

  buf = (char*) calloc(max_test_len, 1);
  assert(buf);

  printf("write\n");

  /* run tests for varying read lengths */
  for (len = min_test_len; len <= max_test_len; len *= 2) {

    /* do enough iterations that it takes at least (target_test_sec) seconds
       to do them all */
    test_time = 0;
    for (n_tests = 1; n_tests < 1000000 && test_time < target_test_sec;
         n_tests *= 2) {
      
      double start_time = getSeconds();
      for (test_iter = 0; test_iter < n_tests; test_iter++) {
        u64 position = myrand(file_size - len);
        fseek(f, position, SEEK_SET);
        write_len = fwrite(buf, len, 1, f);
        if (write_len != 1) {
          fprintf(stderr, "fwrite of %d bytes at %" PRIu64 " failed\n",
                  len, position);
          return;
        }
      }
      test_time = getSeconds() - start_time;

      bytes_per_sec = (u64)len * n_tests / test_time;
      /*
      printf("len %d, %d iters, %.3f sec, %.3f MiB/s\n",
             len, n_tests, test_time, bytes_per_sec / (1024 * 1024));
      */
    }
    
    printf("%d, %f\n", len, bytes_per_sec);
  }
  printf("before free\n");
  free(buf);
  printf("after free\n");
  fclose(f);
  printf("after fclose\n");
}


void computeReadSpeed2(const char *filename, u64 file_size) {
  int len, n_tests, test_iter;
  char *buf;
  double test_time = 0, bytes_per_sec = 0;
  int fd = open(filename, O_RDONLY);
  assert(fd > 0);

  buf = (char*) malloc(max_test_len);
  assert(buf);

  printf("read\n");

  /* run tests for varying read lengths */
  for (len = min_test_len; len <= max_test_len; len *= 2) {

    /* do enough iterations that it takes at least (target_test_sec) seconds
       to do them all */
    test_time = 0;
    for (n_tests = 1; n_tests < 1000000 && test_time < target_test_sec;
         n_tests *= 2) {
      
      double start_time = getSeconds();
      for (test_iter = 0; test_iter < n_tests; test_iter++) {
        u64 position = myrand(file_size - len);
        if (len != pread(fd, buf, len, position)) {
          fprintf(stderr, "pread of %d bytes at %" PRIu64 " failed\n",
                  len, position);
          return;
        }
      }
      test_time = getSeconds() - start_time;

      bytes_per_sec = (u64)len * n_tests / test_time;
      /*
      printf("len %d, %d iters, %.3f sec, %.3f MiB/s\n",
             len, n_tests, test_time, bytes_per_sec / (1024 * 1024));
      */
    }
    
    printf("%d, %f\n", len, bytes_per_sec);
  }
  free(buf);
  close(fd);
}


void computeWriteSpeed2(const char *filename, u64 file_size) {
  int len, n_tests, test_iter;
  char *buf;
  double test_time = 0, bytes_per_sec = 0, start_time;
  int fd = open(filename, O_RDWR);
  assert(fd > 0);

  buf = (char*) calloc(max_test_len, 1);
  assert(buf);

  printf("write\n");

  /* run tests for varying read lengths */
  for (len = min_test_len; len <= max_test_len; len *= 2) {

    /* do enough iterations that it takes at least (target_test_sec) seconds
       to do them all */
    test_time = 0;
    for (n_tests = 1; n_tests < 1000000 && test_time < target_test_sec;
         n_tests *= 2) {
      
      start_time = getSeconds();
      for (test_iter = 0; test_iter < n_tests; test_iter++) {
        u64 position = myrand(file_size - len);
        if (len != pwrite(fd, buf, len, position)) {
          fprintf(stderr, "pwrite of %d bytes at %" PRIu64 " failed\n",
                  len, position);
          return;
        }
      }
      fdatasync(fd);
      test_time = getSeconds() - start_time;

      bytes_per_sec = (u64)len * n_tests / test_time;
      /*
      printf("len %d, %d iters, %.3f sec, %.3f MiB/s\n",
             len, n_tests, test_time, bytes_per_sec / (1024 * 1024));
      */
    }
    
    printf("%d, %f\n", len, bytes_per_sec);
  }
  free(buf);
  start_time = getSeconds();
  close(fd);
  printf("close() took %.3f seconds\n", getSeconds() - start_time);
}
  
