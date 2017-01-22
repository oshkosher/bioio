#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "nexus_parse.h"

#define INITIAL_CAPACITY 1024*1024
#define PAGE_SIZE 4096
#define FILENAME "stringpool.dat"

/* virtual machine cannot mmap a file on a mounted file system */
/* #define FILENAME "/tmp/stringpool.dat" */

typedef struct {
  char *memory;
  size_t size, capacity, mapped_capacity;
  const char *filename;
  int fd, count;
} StringPool;

int StringPool_init(StringPool *pool, const char *filename, size_t capacity);
int StringPool_map(StringPool *pool);
/* returns offset from beginning of the pool where the string is stored */
size_t StringPool_add(StringPool *pool, const char *str);
int StringPool_resize(StringPool *pool, size_t new_capacity);
void StringPool_finish(StringPool *pool);

static size_t roundUpToPageAlignment(size_t size) {
  long page_size = sysconf(_SC_PAGE_SIZE);
  size_t result = size + page_size - 1;
  return result - result % page_size;
}

int StringPool_init(StringPool *pool, const char *filename,
                     size_t capacity) {
  pool->filename = filename;
  pool->capacity = capacity;
  pool->size = 0;
  pool->count = 0;
  pool->fd = -1;

  pool->fd = open(pool->filename, O_RDWR | O_CREAT | O_TRUNC,
                  S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH );
  if (pool->fd == -1) {
    printf("Failed to open %s: %s\n", pool->filename, strerror(errno));
    return errno;
  }

  return StringPool_map(pool);
}


int StringPool_map(StringPool *pool) {
  int err;
  assert(pool->fd != -1);

  pool->mapped_capacity = roundUpToPageAlignment(pool->capacity);

  err = ftruncate(pool->fd, pool->mapped_capacity);
  if (err) {
    printf("Failed to set file size: %s\n", strerror(errno));
    return errno;
  }

  printf("rounding up mapped size to %lu\n",
         (long unsigned) pool->mapped_capacity);
  pool->memory = mmap(NULL, pool->mapped_capacity, PROT_WRITE | PROT_READ,
                      MAP_SHARED, pool->fd, 0);
                      
  if (pool->memory == MAP_FAILED) {
    printf("Failed to map %lu bytes of memory to %s: %s (%d)\n",
           (long unsigned) pool->mapped_capacity, pool->filename,
           strerror(errno),
           errno);
    return errno;
  }

  return 0;
}  


size_t StringPool_add(StringPool *pool, const char *str) {
  size_t len = strlen(str);
  size_t dest = pool->size;

  if (pool->size + len + 1 > pool->capacity) {
    if (StringPool_resize(pool, pool->capacity * 2)) exit(1);
  }
  
  strcpy(pool->memory + pool->size, str);
  pool->size += len + 1;
  pool->count++;

  return dest;
}


int StringPool_resize(StringPool *pool, size_t new_capacity) {
  if (munmap(pool->memory, pool->mapped_capacity)) {
    printf("failed to unmap memory: %s (%d)\n", strerror(errno), errno);
  }
    
  printf("Resizing from %lu to %lu\n", pool->capacity, new_capacity);
  pool->capacity = new_capacity;
  return StringPool_map(pool);
}  


  
  
void StringPool_finish(StringPool *pool) {
  if (munmap(pool->memory, pool->mapped_capacity)) {
    printf("failed to unmap memory: %s (%d)\n", strerror(errno), errno);
  }
  if (ftruncate(pool->fd, pool->size)) {
    printf("failed to truncate %s: %s\n", pool->filename, strerror(errno));
  }
  close(pool->fd);
  pool->fd = -1;
  printf("%d strings, %lu bytes used\n", pool->count,
         (long unsigned) pool->size);
  exit(1);
}
  

int expected_ntaxa = -1;
int current_section_id = -1;
void section_start(void *user_data, int section_id, int line_no, 
                   long file_offset) {
  current_section_id = section_id;
}

void finish_string_pool(StringPool *);
void section_end(void *user_data, int section_id, int line_no,
                 long file_offset) {
  StringPool *pool = (StringPool*) user_data;
  if (current_section_id == NEXUS_SECTION_TAXA) {
    finish_string_pool(pool);
  }
}
  
void setting(void *user_data, NexusSetting *opt) {
  if (current_section_id == NEXUS_SECTION_TAXA) {
    if (!strcasecmp(opt->name, "DIMENSIONS")) {
      NexusSettingPair *pair = opt->setting_list;
      while (pair) {
        if (!strcasecmp(pair->key, "NTAX")) {
          expected_ntaxa = atoi(pair->value);
          printf("Expect %d taxa\n", expected_ntaxa);
          break;
        }
        pair = pair->next;
      }
    }
  }
}
      
  
void taxa_item(void *user_data, const char *name) {
  StringPool *pool = (StringPool*) user_data;

  /* printf("label: %s\n", name); */

  StringPool_add(pool, name);
}

void finish_string_pool(StringPool *pool) {
  printf("%ld bytes used\n", pool->size);
  StringPool_finish(pool);
}


int main(int argc, char **argv) {
  char *filename;
  FILE *inf;
  NexusParseCallbacks callback_functions = {0};
  StringPool pool;

  if (argc != 2) {
    printf("\n  mmap_string_pool <filename | ->\n\n");
    return 1;
  }

  if (StringPool_init(&pool, FILENAME, INITIAL_CAPACITY))
    return 1;
  
  filename = argv[1];
  if (!strcmp(filename, "-")) {
    inf = stdin;
  } else {
    inf = fopen(filename, "r");
    if (!inf) {
      printf("Cannot read \"%s\"\n", filename);
      return 1;
    }
  }

  callback_functions.section_start = section_start;
  callback_functions.section_end = section_end;
  callback_functions.setting = setting;
  callback_functions.taxa_item = taxa_item;

  nexus_parse_file(inf, &pool, &callback_functions);

  return 0;
}

  
