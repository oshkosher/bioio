#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#ifdef __APPLE__
#include <sys/sysctl.h>
#endif

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


int fileExists(const char *filename) {
  struct stat statbuf;

  return !stat(filename, &statbuf);
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

/* Return nonzero if the given filename exists and is a directory. */
int isDirectory(const char *filename) {
  struct stat statbuf;

  return !stat(filename, &statbuf) &&
    S_ISDIR(statbuf.st_mode);
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
  /*
  struct timespec t;
  clock_gettime(CLOCK_MONOTONIC, &t);
  return t.tv_sec + 1e-9 * t.tv_nsec;
  */
  struct timeval t;
  gettimeofday(&t, NULL);
  return t.tv_sec + t.tv_usec * 0.000001;
}


/* Returns the amount of physical memory. */
uint64_t getMemorySize() {
  long page_size, page_count;
  u64 total_mem = 0;

  page_size = sysconf(_SC_PAGE_SIZE);
  page_count = sysconf(_SC_PHYS_PAGES);

  if (page_size > 0 && page_count > 0) {
    total_mem = (u64)page_size * page_count;
  }

#ifdef __APPLE__
  if (total_mem == 0) {
    int mib[2] = { CTL_HW, HW_MEMSIZE };
    unsigned namelen = sizeof(mib) / sizeof(mib[0]);
    u64 size;
    size_t len = sizeof(size);

    if (sysctl(mib, namelen, &size, &len, NULL, 0) < 0) {
      perror("sysctl");
    } else {
      total_mem = size;
    }
  }
#endif

  return total_mem;
}

    
/* Parse a number with a case-insensitive magnitude suffix:
     k : multiply by 1024
     m : multiply by 1024*1024
     g : multiply by 1024*1024*1024

   For example, "32m" would parse as 33554432.
   Return nonzero on error.
*/
int parseSize(const char *str, u64 *result) {
  u64 multiplier = 1;
  
  /* missing argument check */
  if (!str || !str[0]) return 1;

  const char *last = str + strlen(str) - 1;
  char suffix = tolower((int)*last);
  if (suffix == 'k')
    multiplier = 1024;
  else if (suffix == 'm')
    multiplier = 1024*1024;
  else if (suffix == 'g')
    multiplier = 1024*1024*1024;
  else if (!isdigit((int)suffix))
    return 1;

  /* if (multiplier > 1) */
  if (!sscanf(str, "%" SCNu64, result))
    return 1;

  *result *= multiplier;
  return 0;
}


int Array2d_init(Array2d *array, int n_rows, int n_cols, int row_stride) {
  u64 size = (u64)n_rows * row_stride;
  array->data = (char*) malloc(size);
  if (!array->data) {
    fprintf(stderr, "Out of memory. Failed to allocate %" PRIu64 " bytes\n",
            size);
    return -1;
  }

  array->n_rows = n_rows;
  array->n_cols = n_cols;
  array->row_stride = row_stride;
  return 0;
}


static int cache_ob_size = 128;

void transpose(Array2d *dest, int dest_row, int dest_col,
               Array2d *src, int src_row, int src_col,
               int height, int width) {

  if (height > cache_ob_size || width > cache_ob_size) {
    int half;
    if (height > width) {
      half = height / 2;
      transpose(dest, dest_row, dest_col,
                src, src_row, src_col,
                half, width);
      transpose(dest, dest_row, dest_col+half,
                src, src_row+half, src_col,
                height-half, width);
    } else {
      half = width / 2;
      transpose(dest, dest_row, dest_col,
                src, src_row, src_col,
                height, half);
      transpose(dest, dest_row+half, dest_col,
                src, src_row, src_col+half,
                height, width-half);
    }
    return;
  }      

  transposeTile(dest, dest_row, dest_col,
                src, src_row, src_col, height, width);

}


void transposeTile(Array2d *dest, int dest_row, int dest_col,
                   Array2d *src, int src_row, int src_col,
                   int height, int width) {
  int x, y;
  for (x = 0; x < width; x++) {
    for (y = 0; y < height; y++) {
      *Array2d_ptr(dest, dest_row + x, dest_col + y) =
        *Array2d_ptr(src, src_row + y, src_col + x);
    }
  }
}  
