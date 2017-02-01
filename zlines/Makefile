EXECS = create_2d_data zlines zlines_test transpose transpose2

all: $(EXECS)

ZSTD_INC=-Izstd/lib/
ZSTD_LIB=-Lzstd/lib/ -lzstd
ZSTD_LIB_FILE=zstd/lib/libzstd.a

ZLIBS=$(ZSTD_LIB_FILE)
LIBS=-lm
CFLAGS=-g -std=c89 -Wall $(ZSTD_INC)

CC = gcc $(CFLAGS) $(OPT) -D_GNU_SOURCE

zlines: zlines.c zline_api.o common.o
	$(CC) $^ $(ZLIBS) $(LIBS) -o $@

zline_api.o: zline_api.c zline_api.h $(ZSTD_LIB_FILE)
	$(CC) -c $<

common.o: common.c common.h
	$(CC) -c $<

zlines_test: zlines_test.c zline_api.o common.o
	$(CC) $^ $(ZLIBS) $(LIBS) -o $@

transpose: transpose.c common.o
	$(CC) $^ $(LIBS) -o $@

transpose2: transpose2.c common.o
	$(CC) $^ $(LIBS) -o $@

create_2d_data: create_2d_data.c
	$(CC) $^ $(LIBS) -o $@

disk_speed: disk_speed.c common.o
	$(CC) $^ $(LIBS) -o $@


test: create_2d_data transpose test_transpose

test_transpose:
	./create_2d_data 1234 5678 > foo
	./transpose foo bar
	./transpose bar foo2
	shasum foo foo2
	rm foo bar foo2

test_transpose2:
	./create_2d_data 5678 1234 > foo
	./transpose2 -m 20m < foo > bar.1
	./transpose2 -m 4m < foo > bar.2
	./transpose2 -m 500k < foo > bar.3
	shasum bar.1 bar.2 bar.3
#	rm foo bar.1 bar.2 bar.3

# acquire and build the Facebook ZSTD compression library
zstd/lib/zstd.h:
	git clone https://github.com/facebook/zstd.git

$(ZSTD_LIB_FILE): zstd/lib/zstd.h
	(cd zstd; make lib)

clean:
	rm -f $(EXECS) *.o *.stackdump
