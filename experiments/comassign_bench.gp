set terminal png
set output "commassign_bench.png"

set xlabel "Bins"
set ylabel "Relative Frequency"

plot "bench5k.tmp" u 1:3:4 with xerrorbars title "Free slots", \
 "bench5k.tmp" u 1:6:7 with xerrorbars title "Free slots", \
 "bench5k.tmp" u 1:9:10 with xerrorbars title "Free slots"