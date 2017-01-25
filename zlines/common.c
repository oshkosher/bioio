#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>
#include <errno.h>
#include <inttypes.h>
#include "common.h"

typedef uint64_t u64;


/* If filename is "-", return stdin. Otherwise, open the given file
   for reading. */
FILE *openFileOrStdin(const char *filename) {
  FILE *f = stdin;
  
  if (strcmp(filename, "-")) {
    f = fopen(filename, "rt");
    if (!f) {
      fprintf(stderr, "Error: cannot read \"%s\"\n", filename);
    }
  }
  return f;
}


u64 getFileSize(const char *filename) {
   struct stat stats;
   if (stat(filename, &stats)) {
    fprintf(stderr, "Error getting size of \"%s\": %s\n",
	    filename, strerror(errno));
    return 0;
  } else {
    return stats.st_size;
  }
}


/* If for_writing is nonzero, the file will be set to the length *length.
   Otherwise, *length will be set to the length of the file. */
int mapFile(const char *filename, int for_writing, char **data, u64 *length) {
  int fd, flags;
  mode_t mode = 0;

  if (for_writing) {
    flags = O_RDWR | O_CREAT | O_TRUNC;
    mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
  } else {
    flags = O_RDONLY;
    *length = getFileSize(filename);
    if (*length == 0) return -1;
  }

  if (*length >= ((u64)1) << 32 && sizeof(size_t) < 8) {
    fprintf(stderr, "Cannot map large files in 32 bit mode\n");
    return -1;
  }
  
  fd = open(filename, flags, mode);
  if (fd == -1) return -1;

  if (for_writing) {
    if (ftruncate(fd, *length)) {
      fprintf(stderr, "Failed to set the length of %s to %" PRIu64 " bytes\n",
              filename, *length);
      close(fd);
      return -1;
    }
  }
  
  *data = (char*) mmap(NULL, *length, for_writing ? PROT_WRITE : PROT_READ,
                       MAP_SHARED, fd, 0);
  if (*data == MAP_FAILED) {
    fprintf(stderr, "Failed to mmap %s: %s\n", filename, strerror(errno));
    close(fd);
    return -1;
  }

  close(fd);

  return 0;
}


/* format a number with commas every 3 digits: 1234567 -> "1,234,567".
   The string will be written to buf (which must be at least 21 bytes),
   and buf will be returned.
*/
const char *commafy(char *buf, u64 n) {
  unsigned len;
  u64 factor;
  char *p;

  if (n == 0) {
    buf[0] = '0';
    buf[1] = '\0';
    return buf;
  }

  /* find the length */
  len=1;
  factor=1;
  while ((n/factor) >= 10) {
    len++;
    factor *= 10;
  }

  p = buf;
  while (len > 0) {
    int digit = n/factor;
    *p++ = '0' + digit;
    n -= digit * factor;
    len--;
    if (len > 0 && (len % 3) == 0) *p++ = ',';
    factor /= 10;
  }
  *p++ = '\0';

  return buf;
}


double getSeconds() {
  struct timespec t;
  clock_gettime(CLOCK_MONOTONIC, &t);
  return t.tv_sec + 1e-9 * t.tv_nsec;
}
