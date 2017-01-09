#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "nexus_parse.h"

int printHelp();
unsigned long get_memory_used();
double get_time();
int rows_read = 0;
void progress_update();

void my_section_start(void *user_data, int section_id, int line_no, 
                      long file_offset);
void my_section_end(void *user_data, int section_id, int line_no,
                    long file_offset);
void my_tree(void *user_data, const char *name, NewickTreeNode *tree);
void my_setting(void *user_data, NexusSetting *opt);
void my_chars_item(void *user_data, const char *name, const char *data);
void my_crimson_item(void *user_data, const char *name, const char *data);



int main(int argc, char **argv) {
  int result;
  char *filename;
  FILE *inf;
  void *user_data = NULL;
  NexusParseCallbacks callback_functions = {0};

  if (argc != 2) printHelp();

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

  callback_functions.section_start = my_section_start;
  callback_functions.section_end = my_section_end;
  callback_functions.tree = my_tree;
  callback_functions.setting = my_setting;
  callback_functions.chars_item = my_chars_item;
  callback_functions.crimson_item = my_crimson_item;
  
  
  printf("Before parsing, %ld memory in use\n", get_memory_used());
  result = nexus_parse_file(inf, user_data, &callback_functions);

  if (inf != stdin) fclose(inf);
  
  if (result) {
    printf("Errors encountered.\n");
  } else {
    printf("Parse OK.\n");
  }
  
  return 0;
}


double section_start_time;
long section_start_offset;

void my_section_start(void *user_data, int section_id, int line_no, 
                      long file_offset) {
  printf("\n%s section start at line %d, file offset %ld\n", 
         nexus_section_name(section_id), line_no, file_offset);
  section_start_time = get_time();
  section_start_offset = file_offset;
  rows_read = 0;
}

void my_section_end(void *user_data, int section_id, int line_no,
                    long file_offset) {
  double elapsed = get_time() - section_start_time;
  long size = file_offset - section_start_offset;
  double mbps = size / (1024*1024 * elapsed);
  printf("%s section end at line %d\n%ld bytes, parsed in %.3f sec, "
         "%.3f MiB/sec\n", 
         nexus_section_name(section_id), line_no,
         size, elapsed, mbps);
}


void my_tree(void *user_data, const char *name, NewickTreeNode *tree) {
  
  printf("tree %s\n", name);

  /* print the whole tree */
  /* NewickTreeNode_print(tree); */

  /* print a one-line summary of the tree */
  NewickTreeNode_print_summary(tree);

  printf("with tree in memory, %ld memory in use\n", get_memory_used());

  NewickTreeNode_destroy(tree);
}

void my_setting(void *user_data, NexusSetting *opt) {
  NexusSettingPair *pair = opt->setting_list;
  printf("setting %s", opt->name);
  while (pair) {
    printf(" %s=%s", pair->key, pair->value);
    pair = pair->next;
  }
  printf("\n");
  /* NexusSetting_destroy(opt); */
}


/* Callees must deallocate name and data with free(). */
void my_chars_item(void *user_data, const char *name, const char *data) {
  /* printf("chars %s: %s\n", name, data); */
  progress_update();
}

void my_crimson_item(void *user_data, const char *name, const char *data) {
  /* printf("crimson %s: %s\n", name, data); */
  progress_update();
}


unsigned long get_memory_used() {
  FILE *inf = fopen("/proc/self/status", "r");
  char line[128];
  unsigned long mem = 0;

  if (!inf) return 0;

  while (fgets(line, sizeof line, inf) != NULL) {
    if (1 == sscanf(line, "VmSize: %lu", &mem)) {
      mem *= 1024;
      break;
    }
  }

  fclose(inf);
  
  return mem;
}


double get_time() {
  struct timespec t;
  clock_gettime(CLOCK_MONOTONIC, &t);
  return t.tv_sec + 1e-9 * t.tv_nsec;
}


void progress_update() {
  if ((++rows_read % 500000) == 0)
    printf("%d rows read\n", rows_read);
}


int printHelp() {
  printf("\n  read_nexus <input_file>\n"
         "  Use - to read from standard input.\n\n");
  exit(1);
}

