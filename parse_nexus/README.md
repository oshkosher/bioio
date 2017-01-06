# parse_nexus

A tool for parsing "NEXUS" data files.
You'll need "flex" and "bison" installed to compile this.

 - nexus.lex - input file for flex, this splits the input into tokens. It generates
   nexus_lexer.c and nexus_lexer.h.
 - nexus.y - input file for bison, this parses the tokens into structured data. It generates
   nexus.tab.c and nexus.tab.h.
 - nexus_parse.h - this defines the programming interface for someone who is using the parser. The user's code will
   call nexus_parse_file() to start parsing, and the parser will call the other nexus_XXX functions to
   pass the user data from the file.
 - nexus_parse.c - implementation of the programming interface.
 - nexus_parse_stubs.c - "stub" functions that do nothing except deallocate the data passed to them by the parser.
 - read_nexus.c - a simple program that uses the parser to parse a NEXUS file and output some statisics
   about the file and the parser performance.

Here are a couple descriptions of the NEXUS file format.
 - https://en.wikipedia.org/wiki/Nexus_file
 - http://informatics.nescent.org/wiki/NEXUS_Specification

Here are descriptions of the format used to encode trees in the "trees" section
of a NEXUS data file.
 - http://evolution.genetics.washington.edu/phylip/newicktree.html
 - https://en.wikipedia.org/wiki/Newick_format

I tested this on sample data from: http://kim.bio.upenn.edu/software/csd.shtml

