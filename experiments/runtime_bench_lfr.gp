set terminal png
if (exists("TEXBUILD")) set terminal epslatex size 16cm, 8.5cm color colortext
FILEEXT= exists("TEXBUILD") ? ".tex" : "_prev.png"

set output "runtime_bench_lfr" . FILEEXT

set pointsize 2.5
set grid ytics lc rgb "#bbbbbb" lw 1 lt 0
set grid xtics lc rgb "#bbbbbb" lw 1 lt 0

set format x "$10^{%T}$"

set logscale xy
set format y "$10^{%T}$"

set xlabel "Number $n$ of edges"
set ylabel "Runtime [s]"

set xrange [1e6:]

set key bottom right

plot "runtime_bench_lfr_emlfr.dat" u 1:3 w linespoints title "\\emlfr", \
     "runtime_bench_lfr_origlfr.dat" u 1:3 w linespoints title "Original LFR"

