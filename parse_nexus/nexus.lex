 /* FLEX input file describing how to tokenize NEXUS files.
    NEXUS: http://kim.bio.upenn.edu/software/csd.shtml
    FLEX: http://flex.sourceforge.net/manual/index.html

    May 2016, edk@illinois.edu
 */

 /* generate code to update the yylineno variable */
%option yylineno

 /* don't call yywrap at the end of the input */
%option noyywrap

 /* generate reentrant code (thread-safe, no global variables) */
%option reentrant

%option bison-bridge

  /* %option extra-type="YYSTYPE" */

 /* make all parsing case-insensitive */
%option caseless


 /* "start conditions" (token types only valid in some places) */

%x taxa_settings
%x taxa_ident

 /* tokenize the Newick string describing a tree */
%x tree_settings
%x treedef

 /* grab each word in the "characters" section before the matrix */
%x chars_settings

 /* grab the names in the "characters" section */
%x chars_ident

 /* grab the [-AGCU] strings in the "characters" section as one token */
%x chars_str

 /* grab each word in the "crimson" section before the matrix */
%x crimson_settings

 /* grab the [-.()] strings in the "crimson" section as one token */
%x crimson_ident
%x crimson_str

DIGIT    [0-9]
NUMBER   [-+]?([0-9]+(\.[0-9]*)?|[0-9]*\.[0-9]+)([eE][-+]?[0-9]+)?
NAME     [a-zA-Z0-9_]+
WORD     [^ \t\r\n=;]+
WS       [ \n\r\t]*

%{
  #include "nexus_parse.h"
  #include "nexus.tab.h"

  /* Save a pointer to my data in yyextra. */
  #define YY_EXTRA_TYPE ParseVars*

  /* Run this after matching each token to track
     the number of bytes processed. */
  #define YY_USER_ACTION yyextra->byte_offset += yyleng;
  /* #define YY_USER_ACTION yyextra->byte_offset += yyleng; printf("token %s\n", yytext); */
%}

%%

  if (yyextra->new_section > 0) {
    switch (yyextra->new_section) {
    case NEXUS_SECTION_TAXA: BEGIN(taxa_settings); break;
    case NEXUS_SECTION_TREES: BEGIN(tree_settings); break;
    case NEXUS_SECTION_CHARACTERS: BEGIN(chars_settings); break;
    case NEXUS_SECTION_CRIMSON: BEGIN(crimson_settings); break;
    }
    yyextra->new_section = 0;
  }

<taxa_settings>{
  taxlabels BEGIN(taxa_ident); return TAXLABELS;
  {WORD}    yylval->str = strdup(yytext); return WORD;
  "="       return EQUALS;
  ";"       return SEMICOLON;
}

<taxa_ident>{
  {NAME}   yylval->str = strdup(yytext); return NAME;
  ";"      BEGIN(0); return SEMICOLON;
}



<tree_settings>{
  end   BEGIN(0); return END;
  tree  return TREE;
  {WORD}  yylval->str = strdup(yytext); return NAME;
  "="     BEGIN(treedef); return EQUALS;
}

<treedef>{
  "("  return LPAREN;
  ")"  return RPAREN;
  ","  return COMMA;
  ":"  yyextra->after_colon = 1; return COLON;
  {NAME} {
    int result = NAME;
    /* Special hack: an integer is a valid name, but when parsing the label
       for a tree node like "123:456", 123 is the name, and 456 is the length.
       So if we see a number that might be a name but it came just after
       a colon, treat it as a number. */
    if (yyextra->after_colon && 1 == sscanf(yytext, "%lf", &yylval->num)) {
      result = NUMBER;
    } else {
      yylval->str = strdup(yytext);
    }
    yyextra->after_colon = 0;
    return result;
  }
  {NUMBER} {
    yylval->num = atof(yytext);
    yyextra->after_colon = 0;
    return NUMBER;
  }
  ";"  BEGIN(tree_settings); return SEMICOLON;
}

<chars_settings>{
  matrix    BEGIN(chars_ident); return MATRIX;
  {WORD}    yylval->str = strdup(yytext); return WORD;
  "="       return EQUALS;
  ";"       return SEMICOLON;
}

<chars_ident>{
  {NAME}   BEGIN(chars_str); yylval->str = strdup(yytext); return NAME;
  ";"      BEGIN(0); return SEMICOLON;
 }
<chars_str>{
  [-*?A-Za-z]+    BEGIN(chars_ident); yylval->str = strdup(yytext); return CHARS_STR;
}

<crimson_settings>{
  matrix    BEGIN(crimson_ident); return MATRIX;
  {WORD}    yylval->str = strdup(yytext); return WORD;
  "="       return EQUALS;
  ";"       return SEMICOLON;
}

<crimson_ident>{
  {NAME}   BEGIN(crimson_str); yylval->str = strdup(yytext); return NAME;
  ";"      BEGIN(0);  return SEMICOLON;
}

<crimson_str>{
  [-.()]+  BEGIN(crimson_ident); yylval->str = strdup(yytext); return CRIMSON_STR;
}


begin yyextra->begin_byte_offset = yyextra->byte_offset - yyleng; return BEGIN_TOK;
taxa       return TAXA;
trees      return TREES;
characters return CHARACTERS;
crimson    return CRIMSON;
end        return END;
"="        return EQUALS;
";"        return SEMICOLON;

^[ \t]*#[^\n]*\n  /* printf("%d: Comment '%s'\n", yylineno, yytext); */
[ \t\r\n]+        /* printf("%d: whitespace \"%s\"\n", yylineno, yytext); */

<*>"["[^]]*"]"       /* printf("%d: comment '%s'\n", yylineno, yytext); */
<*>{WS}  /* ignore */
<*>.    printf("%d: unexpected character '%s'\n ", yylineno, yytext); return ERR;

