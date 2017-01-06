#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <inttypes.h>
#include <stdint.h>
#include <time.h>

const int BUFFER_SIZE = 1024 * 1024 * 1;

void printHelp();
double getTime();

int main(int argc, char **argv) {
  char *buf;
  int fileno;
  uint64_t size, total_written = 0, write_size;
  double start_time, elapsed, mbps;

  if (argc < 2 || argc > 3) printHelp();

  if (1 != sscanf(argv[1], "%" SCNu64, &size)) {
    fprintf(stderr, "Invalid size: '%s'\n", argv[1]);
    return 1;
  }

  if (argc > 2) {
    fileno = open(argv[2], O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    if (fileno == -1) {
      fprintf(stderr, "Failed to open '%s'\n", argv[2]);
      return 2;
    }
  } else {
    fileno = 1;
  }

  buf = calloc(BUFFER_SIZE, 1);
  if (!buf) {
    fprintf(stderr, "Failed to allocate buffer\n");
    return 3;
  }

  start_time = getTime();
  
  while (total_written < size) {
    if (size - total_written < BUFFER_SIZE)
      write_size = size - total_written;
    else
      write_size = BUFFER_SIZE;

    write_size = write(fileno, buf, write_size);
    if (write_size == -1) {
      fprintf(stderr, "After %" PRIu64 " bytes, write() failed\n", total_written);
      return 3;
    }
    total_written += write_size;
  }
  elapsed = getTime() - start_time;
  mbps = size / (elapsed * 1024 * 1024);

  fprintf(stderr, "write %.1f MiB at %.1f MiB/s\n", size / (1024.0*1024), mbps);

  return 0;
}



void printHelp() {
  fprintf(stderr, "\n  write_speed <size> [filename]\n\n");
  exit(1);
}


double getTime() {
  struct timespec t;
  clock_gettime(CLOCK_MONOTONIC, &t);
  return t.tv_sec + t.tv_nsec * 1e-9;
}
