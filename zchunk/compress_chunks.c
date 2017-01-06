#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <inttypes.h>


#include "zchunk.h"

typedef uint64_t u64;


int CHUNK_SIZE = 4*1024*1024;

void printHelp();
int compressFileChunks(const char *infile_name, const char *outfile_name,
                       const char *outfile_index_name);


int main(int argc, char **argv) {
  const char *infile_name, *outfile_name, *outfile_index_name;
  int err;

  if (argc != 4) printHelp();
  infile_name = argv[1];
  outfile_name = argv[2];
  outfile_index_name = argv[3];

  err = compressFileChunks(infile_name, outfile_name, outfile_index_name);
  
  return err;
}


void printHelp() {
  fprintf(stderr, "\n   compress_chunks <infile> <outfile> <outindex>\n\n");
  exit(1);
}


int compressFileChunks(const char *infile_name, const char *outfile_name,
                        const char *outfile_index_name) {

  unsigned char *inbuf;
  void *outbuf;
  unsigned long bytes_read, outfile_pos = 0, outbuf_len, compressed_size;
  u64 hash;
  FILE *infile, *outfile;
  ZChunkEngine z;
  ZChunkIndex index;
  ZChunkCompressionAlgorithm alg = ZCHUNK_ALG_GZIP;

  zchunkEngineInit(&z, alg, ZCHUNK_DIR_COMPRESS,
                   ZCHUNK_STRATEGY_MAX_COMPRESSION);

  outbuf_len = zchunkMaxCompressedSize(alg, CHUNK_SIZE);
  outbuf = malloc(outbuf_len);

  zchunkIndexInit(&index);
  index.alg = z.alg;
  index.has_hash = 1;

  infile = fopen(infile_name, "rb");
  if (!infile) {
    printf("Cannot read %s\n", infile_name);
    return 1;
  }

  outfile = fopen(outfile_name, "wb");
  if (!outfile) {
    printf("Cannot write %s\n", outfile_name);
    return 1;
  }
  
  /*
  fprintf(outfile_index, "# compression format: %s\n"
          "# offset\tlen\tdecompressed_len\tdecompressed_fnv64_hash\n",
          z.alg == ZCHUNK_ALG_GZIP ? "gzip" : "bzip2");
  */

  inbuf = (unsigned char*) malloc(CHUNK_SIZE);
  if (!inbuf) {
    fprintf(stderr, "Out of memory\n");
    return 1;
  }

  /* loop through chunks */
  while (1) {
    bytes_read = fread(inbuf, 1, CHUNK_SIZE, infile);
    if (bytes_read == 0) break;

    hash = zchunkHash(inbuf, bytes_read);

    compressed_size = zchunkEngineProcess(&z, inbuf, bytes_read,
                                          outbuf, outbuf_len);
                                    
    if (compressed_size == 0)
      return 1;

    fwrite(outbuf, 1, compressed_size, outfile);

    zchunkIndexAdd(&index, bytes_read, compressed_size, hash);
    /*    
    fprintf(outfile_index, "%lu\t%lu\t%lu\t%016" PRIx64 "\n", outfile_pos,
            dest_len, bytes_read, hash);
    */
    fprintf(stderr, "%lu->%lu hash %016" PRIx64 "\n",
            bytes_read, compressed_size, hash);

    outfile_pos += compressed_size;
  }

  zchunkEngineClose(&z);

  zchunkIndexWrite(&index, outfile_index_name);
  zchunkIndexClose(&index);

  free(inbuf);
  free(outbuf);
  fclose(infile);
  fclose(outfile);

  return 0;
}
