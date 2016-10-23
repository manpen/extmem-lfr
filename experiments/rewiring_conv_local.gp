set terminal png

set output key . ".png"

set title key
set logscale y

set xrange [:30]
set yrange [1e-5 : 1e-2]

f(x) = 1e-3

set boxwidth 0.1

plot \
    "conv-". key . "-r0.txt"  using 1:2:3:5:6 with candlesticks lt 1 title "R0" whiskerbars 2, \
    "conv-". key . "-r0.txt"  using 1:4:4:4:4 with candlesticks lt 1 notitle whiskerbars 2, \
    "conv-". key . "-r0.5.txt"  using 1:2:3:5:6 with candlesticks lt 2 title "R0.5" whiskerbars 2, \
    "conv-". key . "-r0.5.txt"  using 1:4:4:4:4 with candlesticks lt 2 notitle whiskerbars 2, \
    "conv-". key . "-r1.0.txt"  using 1:2:3:5:6 with candlesticks lt 3 title "R1" whiskerbars 2, \
    "conv-". key . "-r1.0.txt"  using 1:4:4:4:4 with candlesticks lt 3 notitle whiskerbars 2, \
    "conv-". key . "-r2.0.txt"  using 1:2:3:5:6 with candlesticks lt 4 title "R2" whiskerbars 2, \
    "conv-". key . "-r2.0.txt"  using 1:4:4:4:4 with candlesticks lt 4 notitle whiskerbars 2, \
    f(x) title "0.1 %"
