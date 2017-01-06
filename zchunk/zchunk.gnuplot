# set terminal X11 font "Arial,18"
set terminal png size 600,600 font "Arial,18"
set output "zchunk_compress_speed.png"

#set key outside
set key off
set title "Compression speed"
set xlabel "# of nodes (32 cores each)"
set ylabel "Throughput in GB/s"
# set yrange[0:.8]
# set xrange[1000:1400000]
# set logscale x
# set xtics ("2KB" 2048, "16KB" 16384, "128KB" 131072, "1MB" 1048576, "8MB" 8388608, "64MB" 67108864, "1GB" 1073741824)

plot "zchunk_speed.txt" using 1:2 with linespoints pt 7 ps 3 lw 4 lc rgb "red"

#plot "blocksize_1proc.txt" every ::1 using 1:2 title "1 proc" with linespoints pt 7 ps 3 lw 4,\
#  "blocksize_2proc.txt" every ::1 using 1:2 title "2 procs" with linespoints pt 2 ps 3 lw 4, \
#  "blocksize_4proc.txt" every ::1 using 1:2 title "4 procs" with linespoints pt 5 ps 3 lw 4

set output "zchunk_read_speed.png"
set title "Read speed"
plot "zchunk_speed.txt" using 1:3 with linespoints pt 7 ps 3 lw 4 lc rgb "blue"

