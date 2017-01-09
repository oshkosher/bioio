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

/* forward reference */
struct NexusParseCallbacks;

/* Used internally in the parser and lexer */
typedef struct ParseVars {
  void *user_data;

  /* callback functions */
  struct NexusParseCallbacks *callback;

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


#define NEXUS_SECTION_TAXA 1
#define NEXUS_SECTION_TREES 2
#define NEXUS_SECTION_CHARACTERS 3
#define NEXUS_SECTION_CRIMSON 4

/* Converts a section id to a section name. */
const char *nexus_section_name(int section_id);

/* This structure contains pointers to functions that the parser will
   call to pass results of the parsing to the user. The user should
   should create one of these structures and set the function pointers
   to the their own functions.

   Calling NexusParseCallbacks_init() on one of these structures will
   fill it with functions that do nothing but deallocate memory as
   appropriate.

   All strings are passed as (const char *) arrays, and the user
   should not modify or deallocate them.  For deallocation
   responsibilities of other data structures, check the comments on
   each function.
   
   This would be a lot easier in C++ :-)
*/
typedef struct NexusParseCallbacks {
  /* These will be called at the beginning and end of each section.
     section_id is one of the NEXUS_SECTION_XXX macros defined above. */
  void (*section_start)(void *user_data, int section_id, int line_no,
                        long file_offset);

  void (*section_end)(void *user_data, int section_id, int line_no,
                      long file_offset);

  /* This will be called on each setting line at the top of the sections,
     like this: "dimensions ntax =3 nchar =23;" 
     The called function should not deallocate the setting object.
  */
  void (*setting)(void *user_data, NexusSetting *setting);

  /* This will be called for each entry in the "taxlabels" list in the
     taxa section.
  */
  void (*taxa_item)(void *user_data, const char *name);

  /* This is called on each tree in the tree section.  The callee must
     decallocate the tree with NewickTreeNode_destroy(tree). */
  void (*tree)(void *user_data, const char *name, NewickTreeNode *tree);

  /* This is called on each entry in the matrix list in the characters section.
     The callee must deallocate name and data with free(). */
  void (*chars_item)(void *user_data, const char *name, const char *data);


  /* This is called on each entry in the matrix list in the crimson section.
     The callee must deallocate name and data with free(). */
  void (*crimson_item)(void *user_data, const char *name, const char *data);
} NexusParseCallbacks;


/* Call this to start parsing.

   user_data - this will be passed as the first argument to each of
     the callback functions, so the user can access their own data.
   callbacks - a structure containing pointers to functions that will
     be called by the parser to pass data to the user.
*/
int nexus_parse_file(FILE *inf, void *user_data,
                     struct NexusParseCallbacks *callbacks);

#endif /* __NEWICK_TREE_H__ */
