#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>

#define BUFFER_IN_SIZE (1024*1024)
#define BUFFER_OUT_SIZE (1024*1024)
#define BUFFER_OUT_FLUSH_SIZE (512*1024)

/* like gzip -1 */
/* #define COMPRESSION_LEVEL Z_BEST_SPEED */

/* default gzip compression */
/* #define COMPRESSION_LEVEL Z_DEFAULT_COMPRESSION */

/* like gzip -9 */
#define COMPRESSION_LEVEL Z_BEST_COMPRESSION

/* choose maximum compression window size (15) and make this
   a gzip stream (+16) */
#define WINDOW_BITS (15 + 16)

/* 1 (least memory) to 9 (most memory) */
#define MEM_LEVEL 8

int main(int argc, char **argv) {
  int err, done = 0;
  unsigned char *buffer_in, *buffer_out;
  z_stream z;

  /* {
    int i;
    for (i=10; i <= 1000000000; i *= 10) {
      fprintf(stderr, "compressBound(%d) = %lu\n", i, (unsigned long) compressBound(i));
    }
  } */

  if (argc > 1) {
    stdin = fopen(argv[1], "rb");
    if (argc > 2) {
      stdout = fopen(argv[2], "wb");
    }
  }

  z.zalloc = Z_NULL;
  z.zfree = Z_NULL;
  z.opaque = 0;
  err = deflateInit2(&z, COMPRESSION_LEVEL, Z_DEFLATED, WINDOW_BITS,
                     MEM_LEVEL, Z_DEFAULT_STRATEGY);
  if (err != Z_OK) {
    fprintf(stderr, "Error initializing compression: %s\n",
           err == Z_MEM_ERROR ? "not enough memory"
           : err == Z_STREAM_ERROR ? "invalid compression level"
           : err == Z_VERSION_ERROR ? "invalid library version"
           : "unknown error");
    return 1;
  }
  
  buffer_in = (unsigned char*) malloc(BUFFER_IN_SIZE);
  buffer_out = (unsigned char*) malloc(BUFFER_OUT_SIZE);
  if (!buffer_in || !buffer_out) {
    fprintf(stderr, "Failed to allocate %lu bytes\n",
            (long unsigned) (BUFFER_IN_SIZE + BUFFER_OUT_SIZE));
    return 1;
  }

  z.next_in = buffer_in;
  z.avail_in = 0;

  z.next_out = buffer_out;
  z.avail_out = BUFFER_OUT_SIZE;
  
  while (1) {
    /* int avail_in_prev, avail_out_prev; */
    if (z.avail_in > 0)
      fprintf(stderr, "%d bytes leftover\n", (int)z.avail_in);
    int bytes_read = fread
      (z.next_in + z.avail_in, 1,
       buffer_in + BUFFER_IN_SIZE - (z.next_in + z.avail_in), stdin);
       
    if (bytes_read >= 1)
      z.avail_in += bytes_read;
    else
      done = 1;
    
    /*fprintf(stderr, "read %d bytes\n", bytes_read); */

    /* compress into buffer_out */
    /*
    avail_in_prev = z.avail_in;
    avail_out_prev = z.avail_out;
    */
    err = deflate(&z, done ? Z_FINISH : Z_NO_FLUSH);
    if (err != Z_OK) {
      fprintf(stderr, "deflate says %s\n",
             err == Z_STREAM_END ? "all input and output processed"
             : err == Z_STREAM_ERROR ? "stream state corrupted"
             : err == Z_BUF_ERROR ? "no progress possible"
             : "(unknown)");
      if (err == Z_STREAM_ERROR) break;
    }

    /*
    if (avail_out_prev > z.avail_out)
      fprintf(stderr, "%d compressed bytes output, %d buffered\n",
              (int)(avail_out_prev - z.avail_out),
              (int)(BUFFER_OUT_SIZE - z.avail_out));
    */

    /* flush buffer_out if it has BUFFER_OUT_FLUSH_SIZE bytes in it */
    if (BUFFER_OUT_SIZE - z.avail_out >= BUFFER_OUT_FLUSH_SIZE
        || done) {
      fwrite(buffer_out, 1, BUFFER_OUT_SIZE - z.avail_out, stdout);
      fprintf(stderr, "%d bytes written\n", (int)(BUFFER_OUT_SIZE - z.avail_out));
      z.next_out = buffer_out;
      z.avail_out = BUFFER_OUT_SIZE;
    }

    if (done) break;

    /* move dregs to the top of buffer_in */
    if (z.avail_in) {
      memmove(buffer_in, z.next_in, z.avail_in);
    }
    z.next_in = buffer_in;
  }

  fprintf(stderr, "%ld bytes in, %ld bytes out. "
          "compressBound says max of %ld bytes.\n",
         (unsigned long)z.total_in, (unsigned long)z.total_out,
         (unsigned long) compressBound(z.total_in));
  
  deflateEnd(&z);

  free(buffer_in);
  free(buffer_out);
  return 0;
}
