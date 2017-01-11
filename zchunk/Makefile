EXECS = mygzip compress_chunks compress_chunks_mpi hash_fnv64 \
  zchunk_verify zchunk_verify_mpi write_speed read_speed

all: $(EXECS)

ZSTD_INC=-Izstd/lib/
ZSTD_LIB=-Lzstd/lib/ -lzstd
ZSTD_LIB_DEP=zstd/lib/libzstd.a

ZLIBS=$(ZSTD_LIB) -lz -lbz2
LIBS=-lrt
OPT=-O3 $(ZSTD_INC)
COMPRESSION_ALGS=
COMPRESSION_ALGS+=-DZCHUNK_SUPPORT_GZIP
COMPRESSION_ALGS+=-DZCHUNK_SUPPORT_BZIP
COMPRESSION_ALGS+=-DZCHUNK_SUPPORT_ZSTD

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

mygzip: mygzip.c
	$(CC) $< -lz -o $@

zchunk.o: zchunk.c zchunk.h
	$(CC) $(COMPRESSION_ALGS) -c $<

zchunk_mpi.o: zchunk.c zchunk.h
	$(MPICC) -DZCHUNK_MPI $(COMPRESSION_ALGS) -c $< -o $@

compress_chunks: $(ZSTD_LIB_DEP) compress_chunks.c zchunk.o
	$(CC) $^ $(ZLIBS) -o $@

compress_chunks_mpi: $(ZSTD_LIB_DEP) compress_chunks_mpi.c zchunk_mpi.o
	$(MPICC) -DZCHUNK_MPI $^ $(ZLIBS) $(MPILIB) -o $@

hash_fnv64: $(ZSTD_LIB_DEP) hash_fnv64.c zchunk.o
	$(CC) $^ $(ZLIBS) -o $@

zchunk_verify: $(ZSTD_LIB_DEP) zchunk_verify.c zchunk.o
	$(CC) $^ $(ZLIBS) -o $@

zchunk_verify_mpi: $(ZSTD_LIB_DEP) zchunk_verify_mpi.c zchunk_mpi.o
	$(MPICC) $^ $(ZLIBS) $(MPILIB) -o $@

write_speed: write_speed.c
	$(CC) $< -o $@

read_speed: read_speed.c
	$(CC) $< -o $@

# acquire and build the Facebook ZSTD compression library
zstd/lib/zstd.h:
	git clone https://github.com/facebook/zstd.git

$(ZSTD_LIB_DEP): zstd/lib/zstd.h
	(cd zstd; make)

clean:
	rm -f $(EXECS) *.o nexus_lexer.[ch]
