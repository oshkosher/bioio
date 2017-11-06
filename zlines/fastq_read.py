#!/usr/bin/python

import sys, zlines


def printHelp():
  sys.stderr.write("""
  fastq_read.py <zlines-file> <first-read> <read-count> <which-lines>
    Extract "reads" (blocks of 4 text lines) from the given zlines file,
    printing them to stdout.

    zlines-file: the data file, in 'zlines' format
    first-read: index of the first read, counting from 0
    read-count: number of reads to extract
    which-lines: which of the 4 lines of each read to print, counting from 1
      For example "1" extracts the first line of each read.
      "24" extracts the second and fourth lines of each read.

""")
  sys.exit(1)


def main(args):
  if len(args) != 4: printHelp()

  zlines_filename = args[0]
  first_read = int(args[1])
  read_count = int(args[2])
  which_lines = args[3]

  if first_read < 0:
    sys.stderr.write('Invalid first read: %s\n' % args[1])
    return 1
  if read_count < 0:
    sys.stderr.write('Invalid read count: %s\n' % args[2])
    return 1

  selected = []
  for c in which_lines:
    if c < '1' or c > '4':
      sys.stderr.write('Error in which_lines, only digits 1-4 allowed: "%s"\n'
                       % which_lines)
      return 1
    selected.append(ord(c) - ord('1'))

  f = zlines.open(zlines_filename)
  total_read_count = len(f) / 4

  last_read = min(first_read + read_count, total_read_count)

  read_no = first_read
  while read_no < last_read:
    for s in selected:
      print(f[read_no*4 + s])
    read_no += 1


if __name__ == '__main__':
  sys.exit(main(sys.argv[1:]))
