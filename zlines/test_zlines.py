#!/usr/bin/python3

import sys, zlines, time

# zlineslib = ctypes.cdll.LoadLibrary('libzlines.dll')
# print(repr(zlineslib))

def speedTest():
  if len(sys.argv) != 2:
    print("\n  test_zlines.py <zlines file>\n")
    sys.exit(1)

  start_open = time.time()
  f = zlines.open(sys.argv[1])
  print("%d lines, max len %d" % (len(f), f.max_line_len()))
  max_len = 0
  total_len = 0

  start_read = time.time()
  open_time = start_read - start_open

  for line in f:
    llen = len(line)
    if llen > max_len: max_len = llen
    total_len += llen

  read_time = time.time() - start_read

  print("observed max len %d" % max_len)
  print("open in %.6fs" % open_time);

  block_count = f.block_count()
  total_compressed_size = 0
  for i in range(block_count):
    (c, d) = f.block_size(i)
    total_compressed_size += c

  print("Read %s compressed bytes, %s decompressed bytes in %.6fs" %
        (commafy(total_compressed_size), commafy(total_len), read_time))
  print("%.3f MB/s compressed, %.3f MB/s decompressed\n" %
        (total_compressed_size / (1000000.0*read_time),
         total_len / (1000000.0*read_time)))


def commafy(n):
  n = int(n)
  start = 2 if n < 0 else 1
  s = str(n)
  p = len(s) - 3
  while p >= start:
    s = s[:p] + ',' + s[p:]
    p -= 3
  return s


speedTest()
