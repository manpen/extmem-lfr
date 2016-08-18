set terminal png
if (exists("TEXBUILD")) set terminal epslatex size 12cm, 6.375cm color colortext
FILEEXT= exists("TEXBUILD") ? ".tex" : "_prev.png"

set output "lfr_no_" . fnlabel . FILEEXT

set pointsize 4
set grid ytics lc rgb "#bbbbbb" lw 1 lt 0
set grid xtics lc rgb "#bbbbbb" lw 1 lt 0

set format x "$10^{%T}$"

if (exists("NORMY")) {
    set logscale x
    set yrange [0: 1.1]
} else {
    set logscale xy
    set format y "$10^{%T}$"
}

if (!exists("NORMX")) {
    set xrange [7e2: 6e5]
}

set xlabel "Number $n$ of nodes"

set key autotitle columnhead
plot for [i=0:2] "data/" . fnlabel . ".dat" index i u 2:4:5 w errorbars

