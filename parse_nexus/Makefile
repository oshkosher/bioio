EXECS = read_nexus mmap_string_pool nexus_chars

all: $(EXECS)

LIBS=
OPT=-O3

# Blue Waters
ifeq "$(shell hostname | head -c 8)" "h2ologin"
CC = cc $(OPT)
MPICC = $(CC)
MPILIB=
else
CC = gcc -std=c89 -Wall $(OPT) -D_GNU_SOURCE
MPICC = $(CC) -I/usr/include/mpi
MPILIB=-lmpi
endif

read_nexus: read_nexus.c nexus_lexer.o nexus.tab.o nexus_parse.o
	$(CC) $^ $(LIBS) -o $@

nexus_chars: nexus_chars.c nexus_lexer.o nexus.tab.o nexus_parse.o
	$(CC) $^ $(LIBS) -o $@

testparse: read_nexus example.nex
	./read_nexus example.nex

# Tell flex to make an 8-bit lexer because with a 7-bit lexer it creates
# code that crashes if given garbage input.  This happens because the
# scanner tables are indexed with a varible of type YY_CHAR. In a 7 bit
# lexer, YY_CHAR is just a "char", with may be signed and may have negative
# values, which causes problems when used as an array index. In an 8 bit
# lexer, YY_CHAR is an "unsigned char", which works fine.
nexus_lexer.c: nexus.lex nexus.tab.h
	flex -8 -F --outfile=$@ --header-file=nexus_lexer.h $<

nexus_lexer.o: nexus_lexer.c
	$(CC) -Wno-unused-function -c $<

nexus_parse.o: nexus_parse.c nexus_parse.h
	$(CC) -c $<

nexus.tab.c nexus.tab.h: nexus.y
	bison -d $<

nexus.tab.o: nexus.tab.c nexus.tab.h nexus_parse.h
	$(CC) -c $<

mmap_string_pool: mmap_string_pool.c nexus_lexer.o nexus.tab.o nexus_parse.o
	$(CC) $^ $(LIBS) -o $@

clean:
	rm -f $(EXECS) *~ *.o nexus_lexer.[ch] nexus.tab.[ch] *.stackdump
