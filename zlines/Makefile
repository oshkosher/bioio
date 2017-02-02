EXECS = create_2d_data zlines zlines_test transpose transpose_v0 \
  check_transpose transpose_mmap

all: $(EXECS)

ZSTD_INC=-Izstd/lib/
ZSTD_LIB=-Lzstd/lib/ -lzstd
ZSTD_LIB_FILE=zstd/lib/libzstd.a

ZLIBS=$(ZSTD_LIB_FILE)
LIBS=-lm
OPT=-g
#OPT=-O3
CFLAGS=$(OPT) -std=c89 -Wall $(ZSTD_INC)

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

transpose_v0: transpose_v0.c common.o
	$(CC) $^ $(LIBS) -o $@

transpose_mmap: transpose_mmap.c common.o
	$(CC) $^ $(LIBS) -o $@

check_transpose: check_transpose.c common.o
	$(CC) $^ $(LIBS) -o $@

create_2d_data: create_2d_data.c
	$(CC) $^ $(LIBS) -o $@

disk_speed: disk_speed.c common.o
	$(CC) $^ $(LIBS) -o $@


test: create_2d_data transpose check_transpose test_transpose

test_transpose:
	./create_2d_data 5678 1234 > foo
	./transpose2 -m 20m < foo > bar.1
	./check_transpose foo bar.1
	@echo
	./transpose2 -m 4m < foo > bar.2
	./check_transpose foo bar.2
	@echo
	./transpose2 -m 500k < foo > bar.3
	./check_transpose foo bar.3
	@echo
	rm foo bar.1 bar.2 bar.3

# acquire and build the Facebook ZSTD compression library
zstd/lib/zstd.h:
	git clone https://github.com/facebook/zstd.git

$(ZSTD_LIB_FILE): zstd/lib/zstd.h
	(cd zstd; make lib)

clean:
	rm -f $(EXECS) *.o *.stackdump
