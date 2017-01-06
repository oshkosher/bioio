#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include "zchunk.h"

void printHelp();


int main(int argc, char **argv) {
  const char *data_filename, *index_filename;
  FILE *data_inf;
  ZChunkIndex index;
  int chunk_count, i, bytes_read;
  void *z_buf, *o_buf;
  ZChunkEngine z;
  long unsigned read_pos = 0;
  
  if (argc != 3) printHelp();
  data_filename = argv[1];
  index_filename = argv[2];

  zchunkIndexInit(&index);
  if (zchunkIndexRead(&index, index_filename))
    return 1;

  if (!index.has_hash) {
    printf("No hashes; cannot verify\n");
    return 1;
  }
  
  chunk_count = zchunkIndexSize(&index);

  /* allocate buffers for maximum chunk sizes */
  if (zchunkIndexAllocBuffers(&index, &z_buf, &o_buf))
    return 1;

  zchunkEngineInit(&z, index.alg, ZCHUNK_DIR_DECOMPRESS, 0);

  data_inf = fopen(data_filename, "rb");
  if (!data_inf) {
    printf("Failed to open %s\n", data_filename);
    return 1;
  }
  
  for (i = 0; i < chunk_count; i++) {
    uint64_t o_offset, o_len, z_offset, z_len, z_hash, o_hash;
    zchunkIndexGetOrig(&index, i, &o_offset, &o_len);
    zchunkIndexGetCompressed(&index, i, &z_offset, &z_len, &o_hash);
    
    printf("%d. original at %" PRIu64 ", len %" PRIu64 ", compressed at %"
           PRIu64 ", len %" PRIu64 ", hash %" PRIx64 "\n",
           i, o_offset, o_len, z_offset, z_len, o_hash);

    /* read compressed data */
    bytes_read = fread(z_buf, 1, z_len, data_inf);
    if (bytes_read != z_len) {
      printf("failed to read %d bytes at offset %lu\n", (int)z_len,
             (long unsigned)read_pos);
      return 1;
    }
    read_pos += bytes_read;
    bytes_read = zchunkEngineProcess(&z, z_buf, z_len, o_buf, o_len);
    if (bytes_read != o_len) {
      printf("Expected to decompress %d bytes to %d bytes, but got %d\n",
             (int)z_len, (int)o_len, (int)bytes_read);
      return 1;
    }

    z_hash = zchunkHash(o_buf, o_len);
    if (z_hash != o_hash) {
      printf("  ERR: hash mismatch. Got %" PRIx64 ", expected %" PRIx64 "\n",
             z_hash, o_hash);
    }
  }

  fclose(data_inf);
  zchunkIndexClose(&index);
  zchunkEngineClose(&z);
  free(z_buf);
  free(o_buf);

  return 0;
}


void printHelp() {
  printf("\n  zchunk_verify <zdata> <zindex>\n\n");
  exit(1);
}
