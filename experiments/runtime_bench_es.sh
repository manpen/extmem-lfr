#!/bin/bash
dir="runtime_bench_es"
stats=../../tools/stat.pl

rm -r $dir
mkdir -p $dir/data

cd gengraph_tbone_logs
for f in graph_log*; do
#Degree sequence created. N=46410, 2M=46301022
#Allocate memory for graph...done
#Realize degree sequence... Success
#Convert adjacency lists into hash tables...Done
#Shuffle : 0%2.103939 Performed : 16234 of 23149 swaps
#2.127164 Performed : 32451 of 46298 swaps
#2.150375 Performed : 48666 of 69447 swaps
    tmp="../$dir/$f.tmp"
    nodes=$(head -n1 $f | perl -pe 's/.*N=(\d+),.*/$1/')
    edges=$(head -n1 $f | perl -e '$l = <STDIN>; $l =~ m/.*, 2M=(\d+)/; print($1/2);')

    grep "Performed : " $f | perl -pe 's/.*?(\d+\.\d+) Performed : (\d+) of (\d+) .*/$1 $2 $3/g' > $tmp

    begtime=$( head -n1 $tmp | cut -d" " -f1 -)
    begswaps=$(head -n1 $tmp | cut -d" " -f3 -)

    endtime=$( tail -n1 $tmp | cut -d" " -f1 -)
    endswaps=$(tail -n1 $tmp | cut -d" " -f3 -)
    
    if [ -n "$endtime" ]; then
        esttime=$(perl -e "print(10*$edges / (($endswaps - $begswaps) / ($endtime - $begtime)));")    
        echo "$f:$edges $nodes $begtime $begswaps $endtime $endswaps $esttime"
        echo "$edges $nodes $begtime $begswaps $endtime $endswaps $esttime" >> ../$dir/gengraph.tmp
    fi
done
cd ..

sort -n $dir/gengraph.tmp > $dir/runtime_bench_es1000_gengraph.dat

for f in bench_tbone/*/log_n*; do
    nodes=$(grep -m 1 -e "--num-nodes     (bytes)" $f | perl -pe 's/\D+//g')
    edges=$(grep -m 1 -e "Generated " $f | perl -pe 's/\D+//g')
    swaps=$(grep -m 1 -e "Set numSwaps = " $f | perl -pe 's/\D+//g')
    rtime=$(grep -m 1 -e "SwapStats:  Time since the last reset" $f | perl -pe 's/.* (\d+\.\d+) .*/$1/g')

    
    echo "$f: $edges $nodes $swaps $rtime $esttime" 

    if [ -n "$rtime" ]; then
        esttime=$(perl -e "print($rtime * (10 * $edges / $swaps));")
        echo "$f: $edges $nodes $swaps $rtime $esttime" 
        echo "$edges $nodes $swaps $rtime $esttime" >> $dir/emes.tmp
    else
        echo "$f: Skip"
    fi
done

sort -n $dir/emes.tmp > $dir/runtime_bench_es1000_emes.dat


cd $dir
gnuplot ../runtime_bench_es.gp
