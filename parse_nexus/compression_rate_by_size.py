#!/usr/bin/python3

import sys, os, subprocess, functools

input_file = "run1.chars.1k"
# input_file = "foo"
max_chunk_size = 1000
# max_chunk_size = 10

compression_tools = [
  "gzip",
  "gzip -9",
  "bzip2",
  "zstd --stdout",
]


def main(args):

  line_array = open(input_file).readlines()
  # print('%d lines read' % len(line_array))
  # size = functools.reduce(lambda a,b: a+b, map(lambda line: len(line), line_array))
  # print('%d total length' % size)
  # return 0

  header = ['line count', 'uncompressed bytes'] + compression_tools
  print('\t'.join(header))

  uncompressed_size = os.path.getsize(input_file)
  
  chunk_size = 1
  while chunk_size <= max_chunk_size:

    results = [str(chunk_size), str(uncompressed_size)]
    
    for tool in compression_tools:
      compressed_size = getCompressedSize(chunk_size, tool, line_array)
      # print('%s %d: %d' % (tool, chunk_size, compressed_size))
      ratio = float(uncompressed_size - compressed_size) / uncompressed_size
      results.append("%.5f" % ratio)

    print('\t'.join(results))
    sys.stdout.flush()
    
    next_size = int(chunk_size * 1.1)
    if next_size == chunk_size:
      next_size = chunk_size + 1
    chunk_size = next_size


def getUncompressedSize(chunk_size):
  cmd = "head -%d %s | wc" % (chunk_size, input_file)
  wc_output = subprocess.getoutput(cmd)
  # print(cmd + '\n  ' + wc_output)
  return int(wc_output.split()[2])


def getCompressedSize(chunk_size, tool, line_array):
  start_line = 0
  total_compressed_size = 0

  # print('getCompressedSize %d "%s"' % (chunk_size, tool))
  
  while start_line < len(line_array):
    end_line = min(start_line + chunk_size, len(line_array))
    input_data = ''.join(line_array[start_line:end_line])
    input_data = bytes(input_data, "ascii")

    # print('input_data has %d bytes' % len(input_data))

    proc = subprocess.Popen(tool, shell=True, stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE)
    (out,err) = proc.communicate(input_data)
    compressed_size = len(out)
    total_compressed_size += compressed_size

    # print('lines %d:%d %d bytes' % (start_line, end_line, compressed_size))
    
    start_line += chunk_size

  return total_compressed_size
  

if __name__ == '__main__':
  sys.exit(main(sys.argv[1:]))
