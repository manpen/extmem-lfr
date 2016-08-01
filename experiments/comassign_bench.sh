#!/bin/bash

export OMP_NUM_THREADS=4
./pa_lfr -n 10k -c 500 -i 10 -a 50 -x 20 -y 50 -s 1 -m 0.2 -d 10000 > bench_n10k &
./pa_lfr -n 100k -c 5000 -i 10 -a 500 -x 200 -y 500 -s 2 -m 0.2 -d 5000 > bench_n100k &
./pa_lfr -n 1m -c 5000 -i 10 -a 5k -x 2k -y 5k -s 2 -m 0.2 -d 1000 > bench_n1m &

wait
