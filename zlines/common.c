#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>

#ifdef _WIN32
#include <windows.h>
#include <io.h>
typedef int mode_t;
#define STAT_FN _stat64
#define STAT_STRUCT __stat64
#define S_ISDIR(mode) ((mode) & _S_IFDIR)
#define O_RDWR  _O_RDWR
#define O_RDONLY _O_RDONLY
#define O_CREAT _O_CREAT
#define O_TRUNC _O_TRUNC
#define S_IRUSR _S_IREAD
#define S_IWUSR _S_IWRITE
#define S_IRGRP 0
#define S_IROTH 0
#define REPORT_ERROR(context, isFatal) \
  reportError(__FILE__, __LINE__, context, isFatal);
void reportError(const char *filename, int lineNo,
		 const char *context, int die);
int64_t pread(int fildes, void *buf, uint64_t nbyte, uint64_t offset);
int64_t pwrite(int fildes, const void *buf, uint64_t nbyte, uint64_t offset);
#else
#include <sys/mman.h>
#include <sys/time.h>
#include <unistd.h>
#define STAT_FN stat
#define STAT_STRUCT stat
#endif

#ifdef __APPLE__
#include <sys/sysctl.h>
#endif

#include "common.h"

#define CACHE_OB_SIZE 128
typedef uint64_t u64;

static int File2d_find_byte(File2d *f, char c);

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
  struct STAT_STRUCT statbuf;
  
   if (STAT_FN(filename, &statbuf)) {
    fprintf(stderr, "Error getting size of \"%s\": %s\n",
	    filename, strerror(errno));
    return 0;
  } else {
    return statbuf.st_size;
  }
}

/* Return nonzero if the given filename exists and is a directory. */
int isDirectory(const char *filename) {
  struct STAT_STRUCT statbuf;

  return !STAT_FN(filename, &statbuf) &&
    S_ISDIR(statbuf.st_mode);
}


#ifndef _WIN32

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

#else /* _WIN32 */
int mapFile(const char *filename, int for_writing, char **data, u64 *length) {
  fprintf(stderr, "mapFile not implemented yet on Windows\n");
  return -1;
}
#endif



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
    int digit = (int)(n/factor);
    *p++ = (char)('0' + digit);
    n -= digit * factor;
    len--;
    if (len > 0 && (len % 3) == 0) *p++ = ',';
    factor /= 10;
  }
  *p++ = '\0';

  return buf;
}


#ifdef _WIN32
static LARGE_INTEGER timer_frequency;
double getSeconds(void) {
  static int first_call = 1;
  LARGE_INTEGER counter;

  if (first_call) {
    if (!QueryPerformanceFrequency(&timer_frequency))
      REPORT_ERROR("High-resolution performance counter not supported.", 1);
    first_call = 0;
  }
  
  if (!QueryPerformanceCounter(&counter))
    REPORT_ERROR("calling QueryPerformanceCounter", 1);

  return counter.QuadPart / (double) timer_frequency.QuadPart;
}

#elif __APPLE__
double getSeconds() {
  struct timeval t;
  gettimeofday(&t, NULL);
  return t.tv_sec + t.tv_usec * 0.000001;
}

#else
double getSeconds() {
  struct timespec t;
  clock_gettime(CLOCK_MONOTONIC, &t);
  return t.tv_sec + 1e-9 * t.tv_nsec;
}
#endif


/* Returns the amount of physical memory. */
uint64_t getMemorySize(void) {
#ifdef _WIN32
  MEMORYSTATUSEX memory;

  GlobalMemoryStatusEx(&memory);
  return memory.ullTotalPhys;

#elif __APPLE__
  int mib[2] = { CTL_HW, HW_MEMSIZE };
  unsigned namelen = sizeof(mib) / sizeof(mib[0]);
  u64 size;
  size_t len = sizeof(size);

  if (sysctl(mib, namelen, &size, &len, NULL, 0) < 0) {
    perror("sysctl");
    return 0;
  } else {
    return size;
  }
}

#else  
  long page_size, page_count;

  page_size = sysconf(_SC_PAGE_SIZE);
  page_count = sysconf(_SC_PHYS_PAGES);

  if (page_size > 0 && page_count > 0) {
    return (u64)page_size * page_count;
  } else {
    return 0;
  }
#endif
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
  const char *last;
  char suffix;
  
  /* missing argument check */
  if (!str || !str[0]) return 1;

  last = str + strlen(str) - 1;
  suffix = (char)tolower((int)*last);
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


int File2d_open(File2d *f, const char *filename, int for_writing) {
  int flags;
  mode_t mode = 0;
  u64 length = 0, first_line_len, row, offset;
  char buf[10];

  if (for_writing) {
    flags = O_RDWR | O_CREAT | O_TRUNC;
    mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
    assert(f->n_cols && f->n_rows && f->newline_type);
    f->row_stride = f->n_cols + newlineLength(f->newline_type);
  } else {
    flags = O_RDONLY;
    length = getFileSize(filename);
    if (length == 0) goto fail;
  }

#ifdef _WIN32
    flags |= _O_BINARY;
#endif
  
  f->filename = filename;
  f->fd = open(filename, flags, mode);
  if (f->fd == -1) goto fail;

  if (for_writing) {
    length = (u64)f->n_rows * f->row_stride;
    if (ftruncate(f->fd, length)) {
      fprintf(stderr, "Failed to set the length of %s to %" PRIu64 " bytes\n",
              filename, length);
      goto fail;
    }
  } else {
    assert(length > 0);

    /* guesstimate out the file dimensions */
    first_line_len = File2d_find_byte(f, '\n');
    if (first_line_len >= INT_MAX) {
      fprintf(stderr, "First line of %s is %" PRIu64 " bytes, which is longer "
              "than this tool supports.\n", filename, first_line_len);
      goto fail;
    }

    if (first_line_len == length || first_line_len == -1) {
      fprintf(stderr, "Invalid input file: no line endings found.\n");
      goto fail;
    }

    if (first_line_len == 0) {
      fprintf(stderr, "Invalid input file: first line is empty\n");
      goto fail;
    }

    /* check for DOS (0x0d 0x0a) line endings, since having two bytes at the
       end of each line will change the row stride of the data */
    if (1 != pread(f->fd, buf, 1, first_line_len-1)) {
      fprintf(stderr, "Input file error: read failed\n");
      goto fail;
    }
    f->newline_type = (buf[0] == '\r') ? NEWLINE_DOS : NEWLINE_UNIX;

    f->row_stride = (int)first_line_len + 1;
    f->n_cols = f->row_stride - newlineLength(f->newline_type);

    /* For this tool to work correctly, every line of the file must have
       the same length. Don't check every line, but check that the file
       size is an integer multiple of the line length. */
    
    f->n_rows = (int)(length / f->row_stride);
    if (length != f->n_rows * (u64)f->row_stride) {
      fprintf(stderr, "Invalid input file: uneven line lengths "
              "(rows appear to be %d bytes each, but that doesn't evenly "
              "divide the file length, %" PRIu64 "\n",
              f->row_stride, length);
      goto fail;
    }

    /* Check for the presence of a newline character in the right place
       in a few more rows. */

    for (row=10; row < f->n_rows; row *= 10) {
      offset = File2d_offset(f, (int)row, (int)f->row_stride-1);
      /* printf("check row %d\n", (int)row); */
      if (1 != pread(f->fd, buf, 1, offset)) {
        fprintf(stderr, "Input file error: read failed\n");
        goto fail;
      }
      if (buf[0] != '\n') {
        fprintf(stderr, "Invalid input file: row %d length mismatch\n",
                (int)row);
        return -1;
      }
    }
    
  }

  return 0;

 fail:
  if (f->fd >= 0) close(f->fd);
  f->fd = -1;
  return -1;
}
    

/* Find the first instance of the given byte in the file */
static int File2d_find_byte(File2d *f, char c) {
  char buf[4096], *found_pos;
  u64 pos, original_pos =  lseek(f->fd, 0, SEEK_CUR);
  int read_len;

  pos = lseek(f->fd, 0, SEEK_SET);
  assert(pos == 0);

  do {
    read_len = read(f->fd, buf, sizeof buf);
    if (read_len < 0) return -1;
    found_pos = memchr(buf, c, read_len);
    if (found_pos) {
      lseek(f->fd, original_pos, SEEK_SET);
      return (int)(pos + (found_pos - buf));
    }
    pos += read_len;
  } while (read_len == sizeof buf);

  lseek(f->fd, original_pos, SEEK_SET);
  return -1;
}


/* Get the offset of a location in a File2d */
u64 File2d_offset(File2d *f, int row, int col) {
  return (u64)row * f->row_stride + col;
}
  
  
void File2d_close(File2d *f) {
  close(f->fd);
  f->fd = -1;
}


void transpose(Array2d *dest, int dest_row, int dest_col,
               Array2d *src, int src_row, int src_col,
               int height, int width) {

  if (height > CACHE_OB_SIZE || width > CACHE_OB_SIZE) {
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


void writeNewline(char *dest, int newline_type) {
  if (newline_type == NEWLINE_DOS) {
    dest[0] = '\r';
    dest[1] = '\n';
  } else {
    dest[0] = '\n';
  }
}


#ifdef _WIN32

  
void reportError(const char *filename, int lineNo,
		 const char *context, int die) {
  LPVOID message_buffer;
  FormatMessage(FORMAT_MESSAGE_FROM_SYSTEM |
                FORMAT_MESSAGE_ALLOCATE_BUFFER |
                FORMAT_MESSAGE_IGNORE_INSERTS,
                NULL, GetLastError(), 0, (LPTSTR) &message_buffer, 0, NULL);
  printf("%s:%d Error %d on %s: %s\n", filename, lineNo,
	 GetLastError(), context, message_buffer);
  LocalFree(message_buffer);
  if (die) ExitProcess(1);
}

int64_t pread(int fildes, void *bufv, uint64_t nbyte, uint64_t offset) {
  int64_t bytes_done = 0;
  char *buf = (char*) bufv;
  if (-1 == lseek(fildes, offset, SEEK_SET)) return -1;

  /* handle nbyte > INT_MAX */
  while (nbyte) {
    int result, len = (int)MIN(nbyte, INT_MAX);
    result = read(fildes, buf, len);
    bytes_done += result;
    if (result != len) return bytes_done;
    nbyte -= len;
    buf += len;
  }
  return bytes_done;
}


int64_t pwrite(int fildes, const void *bufv, uint64_t nbyte, uint64_t offset) {
  int64_t bytes_done = 0;
  const char *buf = (const char*) bufv;
  if (-1 == lseek(fildes, offset, SEEK_SET)) return -1;

  /* handle nbyte > INT_MAX */
  while (nbyte) {
    int result, len = (int)MIN(nbyte, INT_MAX);
    result = write(fildes, buf, len);
    bytes_done += result;
    if (result != len) return bytes_done;
    nbyte -= len;
    buf += len;
  }
  return bytes_done;
}


ssize_t getline(char **bufptr, size_t *n, FILE *fp) {
  size_t len = 0;
  char *buf;
  
  assert(bufptr && n && fp);
  if (!*bufptr || *n == 0) {
    *n = 100;
    *bufptr = (char*) malloc(*n);
    assert(*bufptr);
  }

  buf = *bufptr;
  
  while (1) {
    /* stop if we read the EOF before finding a newline */
    if (!fgets(buf + len, *n - len, fp)) break;

    len += strlen(buf + len);

    if (buf[len-1] == '\n') break;

    /* need to extend the buffer */
    *n *= 2;
    *bufptr = realloc(*bufptr, *n);
    assert(*bufptr);
    buf = *bufptr;
  }

  if (len == 0) return -1;

  return (ssize_t)len;
}

#endif /* _WIN32 */
