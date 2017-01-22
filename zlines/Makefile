EXECS = zlines zlines_test

all: $(EXECS)

ZSTD_INC=-Izstd/lib/
ZSTD_LIB=-Lzstd/lib/ -lzstd
ZSTD_LIB_FILE=zstd/lib/libzstd.a

ZLIBS=$(ZSTD_LIB_FILE)
LIBS=-lrt
CFLAGS=-O3 -std=c89 -Wall $(ZSTD_INC)

CC = gcc $(CFLAGS) $(OPT) -D_GNU_SOURCE

zlines: zlines.c zline_api.o
	$(CC) $^ $(ZLIBS) $(LIBS) -o $@

zline_api.o: zline_api.c zline_api.h $(ZSTD_LIB_FILE)
	$(CC) -c $<

zlines_test: zlines_test.c zline_api.o
	$(CC) $^ $(ZLIBS) $(LIBS) -o $@

# acquire and build the Facebook ZSTD compression library
zstd/lib/zstd.h:
	git clone https://github.com/facebook/zstd.git

$(ZSTD_LIB_FILE): zstd/lib/zstd.h
	(cd zstd; make lib)

clean:
	rm -f $(EXECS) *.o *.stackdump
