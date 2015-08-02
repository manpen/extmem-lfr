set terminal pdf;
set output "degree_distr.pdf"

set xlabel "Degree"
set ylabel "Frequency"

set boxwidth 0.5
set style fill solid

set logscale yx

load "degree_distr_title.gp"

plot \
    "dd_logs/init"    u 1:2 with points ls 1 title "Requested",\
    "dd_logs/res"     u 1:2 with dots ls 2 title "HH Dist",\
    "dd_logs/res_rle" u 1:2 with dots ls 3 title "HH RLE"