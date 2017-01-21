EXECS = zlines

all: $(EXECS)

ZSTD_INC=-Izstd/lib/
ZSTD_LIB=-Lzstd/lib/ -lzstd
ZSTD_LIB_DEP=zstd/lib/libzstd.a

ZLIBS=$(ZSTD_LIB)
LIBS=-lrt
CFLAGS=-g -std=c89 -Wall $(ZSTD_INC)

CC = gcc $(CFLAGS) $(OPT) -D_GNU_SOURCE

zlines: zlines.c zline_api.o
	$(CC) $^ $(ZLIBS) $(LIBS) -o $@

zline_api.o: zline_api.c zline_api.h $(ZSTD_LIB_DEP)
	$(CC) -c $<

# acquire and build the Facebook ZSTD compression library
zstd/lib/zstd.h:
	git clone https://github.com/facebook/zstd.git

$(ZSTD_LIB_DEP): zstd/lib/zstd.h
	(cd zstd; make)

clean:
	rm -f $(EXECS) *.o *.stackdump
