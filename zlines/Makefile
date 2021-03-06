ifeq ($(OS),Windows_NT)
  # Cygwin
  SHLIB=libzlines.dll
  ZSTD_SHLIB_ORIG=zstd/lib/dll/libzstd.dll
  ZSTD_SHLIB=libzstd.dll
else
ifeq "$(shell uname)" "Darwin"
  # MacOS
  SHLIB=libzlines.dylib
  ZSTD_SHLIB_ORIG=zstd/lib/libzstd.1.dylib
  ZSTD_SHLIB=libzstd.1.dylib
else
  # Linux
  SHLIB=libzlines.so
  ZSTD_SHLIB_ORIG=zstd/lib/libzstd.so.1
  ZSTD_SHLIB=libzstd.so.1
endif
endif

# windows
# https://stackoverflow.com/questions/2727187/creating-dll-and-lib-files-with-the-vc-command-line
# cl /LD common.c /Fetest.dll
# cl /LD /EHsc /nologo  /O2 /MT -D_SCL_SECURE_NO_WARNINGS -D_CRT_SECURE_NO_WARNINGS /Izstd\lib zline_api.c common.c zstd/build/VS_scripts/bin/Release/x64/libzstd.lib /Felibzlines.dll

EXECS = create_2d_data zlines zlines_test transpose \
  check_transpose transpose_mmap test_zlines fastq_read $(SHLIB)

all: $(EXECS)

ZSTD_INC=-Izstd/lib/
ZSTD_LIB=-Lzstd/lib/ -lzstd
ZSTD_LIB_FILE=zstd/lib/libzstd.a

ZLIBS=$(ZSTD_LIB_FILE)
LIBS=-lm
# OPT=-g
OPT=-O3 -DNDEBUG
CFLAGS=$(OPT) -Wall $(ZSTD_INC)

CC = gcc -std=c89 $(CFLAGS) -D_GNU_SOURCE

zlines: zlines.c zline_api.o common.o
	$(CC) $^ $(ZLIBS) $(LIBS) -o $@

fastq_read: fastq_read.c zline_api.o common.o
	$(CC) $^ $(ZLIBS) $(LIBS) -o $@

zline_api.o: zline_api.c zline_api.h $(ZSTD_LIB_FILE)
	$(CC) -c $<

common.o: common.c common.h
	$(CC) -c $<

zlines_test: zlines_test.c zline_api.o common.o
	$(CC) $^ $(ZLIBS) $(LIBS) -o $@

test_zlines: test_zlines.c zline_api.o common.o
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


test: create_2d_data transpose transpose_v0 check_transpose test_transpose_v0

test_transpose:
	./create_2d_data 5678 1234 > foo
	./transpose -m 20m < foo > bar.1
	./check_transpose foo bar.1
	@echo
	./transpose -m 4m < foo > bar.2
	./check_transpose foo bar.2
	@echo
	./transpose -m 500k < foo > bar.3
	./check_transpose foo bar.3
	@echo
	rm foo bar.1 bar.2 bar.3

test_transpose_v0: transpose_v0
	./create_2d_data 5678 1234 > foo
	./transpose_v0 foo bar
	./check_transpose foo bar
	rm foo bar

$(SHLIB): zline_api.c zline_api.h common.c common.h $(ZSTD_SHLIB)
	$(CC) -fpic -shared -o $@ zline_api.c common.c $(ZSTD_SHLIB)

# acquire and build the Facebook ZSTD compression library
zstd/lib/zstd.h:
	git clone https://github.com/facebook/zstd.git

# build ZSTD
$(ZSTD_LIB_FILE): zstd/lib/zstd.h
	(cd zstd; make lib)

# if the static library is built, the shared one will be too
$(ZSTD_SHLIB_ORIG): $(ZSTD_LIB_FILE)

# copy the shared library to this directory
$(ZSTD_SHLIB): $(ZSTD_SHLIB_ORIG)
	cp $(ZSTD_SHLIB_ORIG) $(ZSTD_SHLIB)

clean:
	rm -rf $(EXECS) *.o *.obj *.exp *.lib *.stackdump *.dSYM *.so *.so.1 \
	  *.dll *.dylib __pycache__ test_zlines.out

