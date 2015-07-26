set terminal pdf;
set output "sweep.pdf"

set xlabel "Number of Nodes"
set ylabel "Runtime [s]"
set y2label "Number of writes"


set xrange [1e5 : 1e10]
set logscale xyy2

set key left
set ytics nomirror
set y2tics

plot \
    "stdout_comp.dat" u 1:3 axes x1y1 w linespoints title "HH classic Runtime", \
    "stdout_comp.dat" u 1:5 axes x1y2 w linespoints title "HH classic IO", \
    "stdoutr_comp.dat"u 1:3 axes x1y1 w linespoints title "HH RLE Runtime", \
    "stdoutr_comp.dat"u 1:5 axes x1y2 w linespoints title "HH RLE IO"