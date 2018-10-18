set terminal png
if (exists("TEXBUILD")) set terminal epslatex size 12cm, 6.375cm color colortext
FILEEXT= exists("TEXBUILD") ? ".tex" : "_prev.png"

set output "rewiring_" . fnlabel . FILEEXT

set pointsize 4
set grid ytics lc rgb "#bbbbbb" lw 1 lt 0
set grid xtics lc rgb "#bbbbbb" lw 1 lt 0

set format x "$10^{%T}$"

set logscale x
set yrange [0 : ]

if (!exists("NORMX")) {
    set xrange [7e2: 1.5e5]
}

set xlabel "Number $n$ of nodes"
set xlabel "Iterations"

set boxwidth 0.1
set key autotitle columnhead

plot for [i=0:1] "glob-" . fnlabel . ".dat" index i using 1:3:2:6:5 with candlesticks lt i+1 whiskerbars 2, \
     for [i=0:1] "glob-" . fnlabel . ".dat" index i using 1:4:4:4:4 with candlesticks lt i+1 notitle whiskerbars 2
