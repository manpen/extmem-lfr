#!/bin/bash
dir="runtime_bench_lfr"
stats=../../tools/stat.pl

rm -r $dir
mkdir -p $dir/data

for f in lfr_benchmark/em/*/*.log; do
    sl=$(grep "Resulting graph" $f | tail -n 1)
    if [ -z "$sl" ]; then
        echo "$f: Skip"
        continue
    fi

    nodes=$(grep -m 1 -e 'nodes set to' $f | perl -pe 's/\D+//g')
    edges=$(echo "$sl" | perl -pe 's/.*has (\d+) edges,.*/$1/g')
    rtime=$(grep -m 1 -e "GlobalGenInitialRand:  Time since the last reset" $f | perl -pe 's/.* (\d+\.\d+) .*/$1/g')
    ltime=$(grep -m 1 -P "LFR:\s+Time since the last reset" $f | perl -pe 's/.*\s(\d+\.\d+|\d+) .*/$1/g')
    
    if [ -n "$ltime" ]; then
        echo "$f: e$edges n$nodes $rtime $ltime" 
                echo "$sl"

        echo "$nodes $edges $rtime $ltime" >> $dir/emlfr.tmp
    else
        echo "$f: Skip"
    fi
done

sort -n $dir/emlfr.tmp > $dir/runtime_bench_lfr_emlfr.dat

for f in lfr_benchmark/orig/*/*.log; do
    sl=$(grep "average mixing parameter:" $f)
    if [ -z "$sl" ]; then
        echo "$f: Skip"
        continue
    fi

    nodes=$(grep -m 1 -e "number of nodes:" $f | perl -pe 's/\D+//g')
    edges=$(grep -m 1 -e "network of " $f | perl -pe 's/.*and (\d+) edges;.*/$1/g')
    ltime=$(grep -m 1 -P "(wall clock)" $f | perl -pe 's/.*\)://g')
    
    itime=$(echo "$ltime" | perl -e '$l = <STDIN>; $l =~ m/$\s*((\d+):)?(\d+):(\d+\.?\d*)/; print(3600 * $2 + 60 * $3 + $4);')
    
    if [ -n "$itime" ]; then
        echo "$f: $edges n$nodes $ltime $itime"
        echo "$nodes $edges $itime" >> $dir/origlfr.tmp
    else
        echo "$f: Skip"
    fi
done


sort -n $dir/origlfr.tmp > $dir/runtime_bench_lfr_origlfr.dat

cd $dir
gnuplot ../runtime_bench_lfr.gp