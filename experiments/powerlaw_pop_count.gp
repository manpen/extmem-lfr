set terminal png
set output "gpop_count.png"

datfile="powerlaw_pop_count.dat"


set key left

set xrange [600 : 1e8]
set yrange [8 : 6e5]

set logscale xy

xdatmin=1000
xdatmax=6.3e7

invalid(x) = (x < xdatmin) || (x > xdatmax) ? 1 : 0

ea=1
eb=1
ef(x)= invalid(x) ? 1/0 : ea* x ** eb
fit [xdatmin:xdatmax] ef(x) datfile index 0 u 2:4 via ea, eb

fa=1
ff(x)= invalid(x) ? 1/0 : fa*x**0.5
fit [xdatmin:xdatmax] ff(x) datfile index 1 u 2:4 via fa

ga=1
gf(x)= invalid(x) ? 1/0 : ga*x**0.333
fit [xdatmin:xdatmax] gf(x) datfile index 2 u 2:4 via ga

plot \
   datfile index 0 u 2:4:5 w yerrorbars lt 1 title sprintf("$\gamma{=}1$, fit: $f(x){=}%.3f \cdot x^{1/1}$", ea), ef(x) lt 1 notitle, \
   datfile index 1 u 2:4:5 w yerrorbars lt 2 title sprintf("$\gamma{=}2$, fit: $f(x){=}%.3f \cdot x^{1/2}$", fa), ff(x) lt 2 notitle, \
   datfile index 2 u 2:4:5 w yerrorbars lt 3 title sprintf("$\gamma{=}3$, fit: $f(x){=}%.3f \cdot x^{1/3}$", ga), gf(x) lt 3 notitle
