#ifndef __NEWICK_TREE_H__
#define __NEWICK_TREE_H__

#include <stdio.h>

/* Data structures used to encapsulate the parsed data. */

typedef struct NewickTreeNode {
  char *name;
  double length;
  struct NewickTreeNode *parent, *child, *sibling;
} NewickTreeNode;

NewickTreeNode* NewickTreeNode_create(const char *name, double length);
void NewickTreeNode_add_child(NewickTreeNode *node, NewickTreeNode *child);
void NewickTreeNode_add_sibling(NewickTreeNode *node, NewickTreeNode *sibling);
void NewickTreeNode_print(NewickTreeNode *node);
void NewickTreeNode_print_summary(NewickTreeNode *node);
void NewickTreeNode_destroy(NewickTreeNode *node);


typedef struct NexusSettingPair {
  char *key, *value;
  struct NexusSettingPair *next;
} NexusSettingPair;

typedef struct NexusSetting {
  char *name;
  NexusSettingPair *setting_list;
} NexusSetting;

NexusSetting *NexusSetting_create(const char *name);
void NexusSetting_add(NexusSetting *opt, const char *key, const char *value);
void NexusSetting_destroy(NexusSetting*);

/* Used internally in the parser and lexer */
typedef struct ParseVars {
  void *user_data;

  /* used by parser */
  NexusSetting *current_setting;

  /* if this is nonzero, it is one of the NEXUS_SECTION_* constants, and
     the lexer will switch to the appropriate mode. */
  int new_section;

  /* offset of most recent BEGIN token */
  long begin_byte_offset;

  /* used by lexer */
  int after_colon;
  long byte_offset;
} ParseVars;


/* Call this to start parsing.

   The value passed as user_data will be passed as the first argument
   to each of the callback functions below. You can use this to pass
   a pointer to your own data to each of the callbacks.
*/
int nexus_parse_file(FILE *inf, void *user_data);

#define NEXUS_SECTION_TAXA 1
#define NEXUS_SECTION_TREES 2
#define NEXUS_SECTION_CHARACTERS 3
#define NEXUS_SECTION_CRIMSON 4

/* Converts a section id to a section name. */
const char *nexus_section_name(int section_id);


/* These functions will be called by the parser to pass the results
   of the parsing.  The user's code must implement all of these.
   Empty stubs can be copied from nexus_parse_stubs.c. */

/* These will be called at the beginning and end of each section. */
void nexus_section_start(void *user_data, int section_id, int line_no,
                         long file_offset);
void nexus_section_end(void *user_data, int section_id, int line_no,
                       long file_offset);

/* This will be called on each setting line at the top of the sections,
   like this: "dimensions ntax =3 nchar =23;" 
   The callee must deallocate the object with NexusSetting_destroy(opt).
*/
void nexus_setting(void *user_data, NexusSetting *opt);

/* This will be called for each entry in the "taxlabels" list in the
   taxa section.
   The callee must deallocate the string with free(name).
*/
void nexus_taxa_label(void *user_data, char *name);

/* This is called on each tree in the tree section.
   The callee must decallocate the name string with free(name) and
   the tree with NewickTreeNode_destroy(tree); */
void nexus_tree(void *user_data, char *name, NewickTreeNode *tree);

/* This is called on each entry in the matix list in the characters section.
   The callee must deallocate name and data with free(). */
void nexus_chars_entry(void *user_data, char *name, char *data);

/* This is called on each entry in the matix list in the crimson section.
   The callee must deallocate name and data with free(). */
void nexus_crimson_entry(void *user_data, char *name, char *data);


#endif /* __NEWICK_TREE_H__ */
