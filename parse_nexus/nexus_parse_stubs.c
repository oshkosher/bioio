#include <stdlib.h>
#include "nexus_parse.h"

void nexus_section_start(void *user_data, int section_id, int line_no,
                         long file_offset) {}

void nexus_section_end(void *user_data, int section_id, int line_no,
                       long file_offset) {}

void nexus_setting(void *user_data, NexusSetting *opt) {
  NexusSetting_destroy(opt);
}

void nexus_taxa_label(void *user_data, char *name) {
  free(name);
}

void nexus_tree(void *user_data, char *name, NewickTreeNode *tree) {
  free(name);
  NewickTreeNode_destroy(tree);
}

void nexus_chars_entry(void *user_data, char *name, char *data) {
  free(name);
  free(data);
}

void nexus_crimson_entry(void *user_data, char *name, char *data) {
  free(name);
  free(data);
}
