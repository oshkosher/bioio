%token BEGIN_TOK TAXA TREES CHARACTERS CRIMSON MATRIX TAXLABELS END
%token TREE DIMENSIONS FORMAT
%token SEMICOLON EQUALS COLON COMMA LPAREN RPAREN
%token NAME WORD CHARS_STR CRIMSON_STR
%token NUMBER
%token ERR

%type <node> tree_node node_ident node_list child_list
%type <str> NAME WORD CHARS_STR CRIMSON_STR
%type <num> NUMBER

%{
  #include <stdio.h>
  #include "nexus_parse.h"

  int yyget_lineno(void *scanner);
  int yyget_leng(void *scanner);
%}

%union {
  int i;
  char *str;
  double num;
  struct NewickTreeNode *node;
}

 /* generate reentrant code */
%pure-parser
%lex-param  {void * scanner}
%parse-param  {void * scanner}
%parse-param  {ParseVars *parse_vars}


%{
  int yylex(YYSTYPE *lval, void* scanner);
  int yyerror(void* scanner, ParseVars *parse_vars, const char *errmsg);
%}

%%

file: file section | /* empty */;

section: taxa_section | tree_section | chars_section | crimson_section ;

taxa_section:
  BEGIN_TOK TAXA SEMICOLON
  {parse_vars->new_section = NEXUS_SECTION_TAXA;
   parse_vars->callback->section_start
      (parse_vars->user_data, NEXUS_SECTION_TAXA, yyget_lineno(scanner),
       parse_vars->begin_byte_offset);}
  setting_lines TAXLABELS taxa_list SEMICOLON END SEMICOLON
  {parse_vars->callback->section_end
      (parse_vars->user_data, NEXUS_SECTION_TAXA, yyget_lineno(scanner),
       parse_vars->byte_offset);}
  ;

taxa_list:
  taxa_list NAME {
      parse_vars->callback->taxa_item(parse_vars->user_data, $2);
      free($2);
    }
  | /* empty */ ;


tree_section:
  BEGIN_TOK TREES SEMICOLON
  {parse_vars->new_section = NEXUS_SECTION_TREES;
   parse_vars->callback->section_start
     (parse_vars->user_data, NEXUS_SECTION_TREES, yyget_lineno(scanner),
      parse_vars->begin_byte_offset);}
  tree_list END SEMICOLON
  {parse_vars->callback->section_end
      (parse_vars->user_data, NEXUS_SECTION_TREES, yyget_lineno(scanner),
       parse_vars->byte_offset);}
  ;

tree_list:
  TREE NAME EQUALS tree_node SEMICOLON {
    parse_vars->callback->tree(parse_vars->user_data, $2, $4);
    free($2);
  }
  tree_list
  | /* empty */ ;

tree_node: child_list node_ident {
    if ($1 != NULL) {
      NewickTreeNode_add_child($2, $1);
    }
    $$ = $2;
  }

node_ident:
    NAME COLON NUMBER {
      /* name and length */
      $$ = NewickTreeNode_create($1, $3);
      free($1);
    }
  | NAME {
      /* length omitted */
      $$ = NewickTreeNode_create($1, -1);
      free($1);
    }
  | COLON NUMBER {
      /* name omitted */
      $$ = NewickTreeNode_create("", $2);
    }
  | /* empty */ {
      /* name and length omitted */
      $$ = NewickTreeNode_create("", -1);
    }
  ;

child_list:
  LPAREN node_list RPAREN {$$ = $2;}
  | /* empty */  {$$ = NULL;}
  ;


node_list:
    node_list COMMA tree_node {
      NewickTreeNode_add_sibling($1, $3);
      $$ = $1;
    }
  | tree_node {
      $$ = $1;
    }
  ;


chars_section:
  BEGIN_TOK CHARACTERS SEMICOLON
  {parse_vars->new_section = NEXUS_SECTION_CHARACTERS;
   parse_vars->callback->section_start
     (parse_vars->user_data, NEXUS_SECTION_CHARACTERS, yyget_lineno(scanner),
      parse_vars->begin_byte_offset);}
  setting_lines MATRIX chars_list SEMICOLON END SEMICOLON
  {parse_vars->callback->section_end
      (parse_vars->user_data, NEXUS_SECTION_CHARACTERS, yyget_lineno(scanner),
       parse_vars->byte_offset);}
  ;

/* dimensions... format... */
setting_lines:
    setting_lines WORD {
      parse_vars->current_setting = NexusSetting_create($2);
      free($2);
    }
    option_list SEMICOLON {
      parse_vars->callback->setting(parse_vars->user_data,
                                    parse_vars->current_setting);
      NexusSetting_destroy(parse_vars->current_setting);
      parse_vars->current_setting = NULL;
    }
  | /* empty */ ;


/* natx=.. nchar=... */
option_list:
    option_list WORD EQUALS WORD {
      /* printf("  %s = %s\n", $2, $4); */
      NexusSetting_add(parse_vars->current_setting, $2, $4);
      free($2);
      free($4);
    }
  | /* empty */ ;


/* name1  ACG...
   name2  GC-UA... */
chars_list:
    chars_list NAME CHARS_STR {
      parse_vars->callback->chars_item(parse_vars->user_data, $2, $3);
      free($2);
      free($3);
    }
  | /* empty */ ;


crimson_section:
  BEGIN_TOK CRIMSON SEMICOLON
  {parse_vars->new_section = NEXUS_SECTION_CRIMSON;
   parse_vars->callback->section_start
     (parse_vars->user_data, NEXUS_SECTION_CRIMSON, yyget_lineno(scanner),
      parse_vars->begin_byte_offset);}
  setting_lines MATRIX crimson_list SEMICOLON END SEMICOLON
  {parse_vars->callback->section_end
      (parse_vars->user_data, NEXUS_SECTION_CRIMSON, yyget_lineno(scanner),
       parse_vars->byte_offset);}
  ;


/* name1  ..((..))...
   name2  ....(((.))) */
crimson_list:
    crimson_list NAME CRIMSON_STR {
      parse_vars->callback->crimson_item(parse_vars->user_data, $2, $3);
      free($2);
      free($3);
    }
  | /* empty */ ;

