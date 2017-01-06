# In the Nexus data files, in the "characters" sections, there are long
# strings of base pair charaters like:
#  ---C---G-----C-----....
# To save file space and improve I/O throughput, it may be beneficial to
# compress this data. Since compression works best on larger files, I wanted
# to see if compressing each line individually would be beneficial, and if not,
# what would the compression rates be if we compressed multiple lines at
# a time.

# Results in a nutshell
#  - With gzip, it compresses this data to about 10% of its original size
#    for all input sizes, even a single line.
#  - With gzip -9, which is much slower, it compresses the data to about
#    3.8% of its original size, but needs at least 10 lines of data in
#    each compressed chunk.
#  - With both bzip2 and zstd (the Facebook compression algorithm, see
#    https://github.com/facebook/zstd), they compress to 1.3% of the original
#    size, and need about 100 lines of data in each compressed chunk.
#    With chunks of 10 lines, they compress to 2.5% of the original size.
# In the data file tested, each line of data was about 20kb.

# The original data file is from:
# http://kim.bio.upenn.edu/web/software/cipres/simulated/1m/run1.nex.bz2
# After decompressing it, I extracted just the data from the "characters"
# section with this:
#
#    extract run1.nex 62169538 43917978138 | tail -n +5 | head -1000 | awk '{print $2}' > run1.chars.1k
#
# "extract" is a tool to extract a subrange of bytes from a file.
# It should be in this repository somewhere, possibly in the parent directory
# of this file.

# Run the experiment:
#   compression_rate_by_size.py > compression_rate_by_size.out

set term x11 font "Sans,18"
set xlabel "Number of lines per compressed chunk,\neach line is 20kb"
set ylabel "Size of compressed data,\nas a fraction of original size"
set logscale x
plot \
  "compression_rate_by_size.out" using 1:(1-$3) title "gzip", \
  "compression_rate_by_size.out" using 1:(1-$4) title "gzip -9", \
  "compression_rate_by_size.out" using 1:(1-$5) title "bzip2", \
  "compression_rate_by_size.out" using 1:(1-$6) title "zstd"
