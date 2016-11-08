set title "Benchmark ConfigurationModel"

set xlabel "Edges"
set ylabel "Time [s]"

set terminal pdf
set output 'cm_benchmark_mout.pdf'
set logscale xy
set style line 1 lc rgb '#0060ad' lt 1 lw 2 pt 7 ps 1.5   # --- blue
set style line 2 lc rgb '#ff0000' lt 1 lw 2 pt 7 ps 1.5   # --- red
plot 'runtime_cm_crc.dat' with linespoints ls 1 , \
     'runtime_cm_r.dat' with linespoints ls 2
