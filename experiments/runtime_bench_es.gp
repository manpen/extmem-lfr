set terminal png
if (exists("TEXBUILD")) set terminal epslatex size 16cm, 8.5cm color colortext
FILEEXT= exists("TEXBUILD") ? ".tex" : "_prev.png"

set output "plot_es_runtime" . FILEEXT

set pointsize 2.5
set grid ytics lc rgb "#bbbbbb" lw 1 lt 0
set grid xtics lc rgb "#bbbbbb" lw 1 lt 0

set format x "$10^{%T}$"

set logscale xy
set format y "$10^{%T}$"

set xlabel "Number $m$ of edges"
set ylabel "Runtime [s]"

set key left

plot "runtime_bench_es1000_gengraph.dat" u 1:7 w linespoints title "\\esvl", \
     "runtime_bench_es1000_emes.dat" u 1:5 w linespoints title "\\estfp"

