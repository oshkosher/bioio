#include <stdio.h>
#include <stdint.h>
#include <inttypes.h>
#include "zchunk.h"

typedef uint64_t u64;

u64 hashStream(FILE *inf);
int hashFile(const char *filename, u64 *hash);


int main(int argc, char **argv) {
  u64 hash;
  int i;

  if (argc == 1) {
    hash = hashStream(stdin);
    printf("%016" PRIx64 "\n", hash);
  } else {
    for (i = 1; i < argc; i++) {
      if (hashFile(argv[i], &hash))
        printf("%016" PRIx64 "  %s\n", hash, argv[i]);
    }
  }
  return 0;
}


u64 hashStream(FILE *inf) {
  char buf[4096];
  int bytes_read;
  u64 hash = zchunkHash(NULL, 0);

  while ((bytes_read = fread(buf, 1, sizeof buf, inf)) > 0) {
    hash = zchunkHashContinue(buf, bytes_read, hash);
  }

  return hash;
}


int hashFile(const char *filename, u64 *hash) {
  FILE *inf = fopen(filename, "rb");
  if (!inf) {
    printf("Cannot read \"%s\"\n", filename);
    return 0;
  }

  *hash = hashStream(inf);
  fclose(inf);
  return 1;
}

