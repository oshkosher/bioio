#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdio.h>
#include "nexus_parse.h"
#include "nexus.tab.h"
#include "nexus_lexer.h"

/* just in case you're using an old version of bison that doesn't include
   this in nexus.tab.h */
int yyparse (void * scanner, ParseVars *parse_vars);

int nexus_parse_file(FILE *inf, void *user_data) {
  yyscan_t scanner;
  int result;
  ParseVars parse_vars = {user_data, NULL, 0, 0};

  yylex_init(&scanner);
  yylex_init_extra(&parse_vars, &scanner);
  yyset_in(inf, scanner);
  result = yyparse(scanner, &parse_vars);
  yylex_destroy(scanner);

  return result;
}


const char *nexus_section_name(int section_id) {
  switch (section_id) {
  case NEXUS_SECTION_TAXA: return "taxa";
  case NEXUS_SECTION_TREES: return "tree";
  case NEXUS_SECTION_CHARACTERS: return "characters";
  case NEXUS_SECTION_CRIMSON: return "crimson";
  default: return "unknown";
  }
}

void yyerror(void *scanner, const char *err) {
  /* printf("%d: %s\n", yyget_lineno(scanner), err); */
  printf("Sytax error, line %d at \"%s\"\n", yyget_lineno(scanner),
         yyget_text(scanner));
}


NewickTreeNode* NewickTreeNode_create(const char *name, double length) {
  NewickTreeNode *node = (NewickTreeNode*) malloc(sizeof(NewickTreeNode));
  if (name == NULL) {
    node->name = NULL;
  } else {
    node->name = (char*) malloc(strlen(name) + 1);
    strcpy(node->name, name);
  }
  node->length = length;
  node->parent = node->child = node->sibling = NULL;

  return node;
}


void NewickTreeNode_add_child(NewickTreeNode *node, NewickTreeNode *child) {
  NewickTreeNode *p;

  /* first set all the new kids' parents */
  for (p = child; p != NULL; p = p->sibling)
    p->parent = node;

  if (!node->child) {
    /* if I have no children, just assign this list */
    node->child = child;
  } else {
    /* if I have children, append this one */
    NewickTreeNode_add_sibling(node->child, child);
  }
}  
  

void NewickTreeNode_add_sibling(NewickTreeNode *node, NewickTreeNode *sibling) {
  /* find the end of the list of siblings */
  while (node->sibling != NULL)
    node = node->sibling;
  node->sibling = sibling;
}


static void NewickTreeNode_print_recurse(NewickTreeNode *node, int depth) {
  NewickTreeNode *child = node->child;
  printf("%*s%s", depth, "", node->name[0] ? node->name : "no-name");
  if (node->length >= 0)
    printf(":%f\n", node->length);
  else
    putchar('\n');
  while (child) {
    if (child->parent != node) {
      printf("ERROR: parent of %s is %s, should be %s\n",
             child->name, (child->parent==NULL) ? "NULL" : child->parent->name,
             node->name);
    }
    NewickTreeNode_print_recurse(child, depth+1);
    child = child->sibling;
  }
}


void NewickTreeNode_print(NewickTreeNode *node) {
  NewickTreeNode_print_recurse(node, 0);
}


typedef struct {
  int internal_nodes, leaves;
  int child_count, height;
} TreeStats;


void NewickTreeNode_stats(NewickTreeNode *node, TreeStats *stats,
                          int depth) {
  NewickTreeNode *child = node->child;
  int n_children = 0;
  if (depth > stats->height) stats->height = depth;
  if (child) {
    stats->internal_nodes++;
    while (child) {
      n_children++;
      NewickTreeNode_stats(child, stats, depth+1);
      child = child->sibling;
    }
    stats->child_count += n_children;
  } else {
    stats->leaves++;
  }
}


void NewickTreeNode_print_summary(NewickTreeNode *node) {
  TreeStats stats = {0, 0, 0, 0};
  NewickTreeNode_stats(node, &stats, 1);
  
  printf("root node %s, %d internal nodes averaging %.2f children, "
         "%d leaves, height %d\n",
         node->name[0] ? node->name : "no-name", stats.internal_nodes,
         (double)stats.child_count / stats.internal_nodes,
         stats.leaves, stats.height);
}


void NewickTreeNode_destroy(NewickTreeNode *node) {
  NewickTreeNode *next;

  while (node) {
    next = node->sibling;
  
    free(node->name);
    node->name = NULL;

    NewickTreeNode_destroy(node->child);
    free(node);
    node = next;
  }
}


NexusSetting *NexusSetting_create(const char *name) {
  NexusSetting *opt = (NexusSetting*) malloc(sizeof(NexusSetting));
  opt->name = name ? strdup(name) : NULL;
  opt->setting_list = NULL;
  return opt;
}


void NexusSetting_destroy(NexusSetting *opt) {
  NexusSettingPair *pair = opt->setting_list, *next;
  free(opt->name);
  while (pair) {
    next = pair->next;
    free(pair->key);
    free(pair->value);
    free(pair);
    pair = next;
  }
  free(opt);
}


void NexusSetting_add(NexusSetting *opt, const char *key, const char *value) {
  NexusSettingPair *pair = (NexusSettingPair*) malloc(sizeof(NexusSettingPair));
  pair->key = strdup(key);
  pair->value = strdup(value);
  pair->next = NULL;
  
  if (!opt->setting_list) {
    opt->setting_list = pair;
  } else {
    /* attach the new pair to the end */
    NexusSettingPair *tail = opt->setting_list;
    while (tail->next) tail = tail->next;
    tail->next = pair;
  }
}
