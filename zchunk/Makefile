EXECS = mygzip compress_chunks compress_chunks_mpi hash_fnv64 \
  zchunk_verify zchunk_verify_mpi write_speed read_speed

all: $(EXECS)

LIBS=-lrt
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

mygzip: mygzip.c
	$(CC) $< -lz -o $@

zchunk.o: zchunk.c zchunk.h
	$(CC) -c $<

zchunk_mpi.o: zchunk.c zchunk.h
	$(MPICC) -DZCHUNK_MPI -c $< -o $@

compress_chunks: compress_chunks.c zchunk.o
	$(CC) $^ -lz -lbz2 -o $@

compress_chunks_mpi: compress_chunks_mpi.c zchunk_mpi.o
	$(MPICC) -DZCHUNK_MPI $^ -lz -lbz2 $(MPILIB) -o $@

hash_fnv64: hash_fnv64.c zchunk.o
	$(CC) $^ -lz -lbz2 -o $@

zchunk_verify: zchunk_verify.c zchunk.o
	$(CC) $^ -lz -lbz2 -o $@

zchunk_verify_mpi: zchunk_verify_mpi.c zchunk_mpi.o
	$(MPICC) $^ -lz -lbz2 $(MPILIB) -o $@

write_speed: write_speed.c
	$(CC) $< -o $@

read_speed: read_speed.c
	$(CC) $< -o $@

clean:
	rm -f $(EXECS) *.o nexus_lexer.[ch]
