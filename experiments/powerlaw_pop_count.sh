#!/usr/bin/env bash

echo "Simulate"

dir="powerlaw_pop_count"
gammas="1 2 3"
iterations=9

rm -r $dir
mkdir $dir
cd $dir

for n in \
   1000 1584 2511 3981 6309 \
   10000 15840 25110 39810 63090 \
   100000 158400 251100 398100 630900 \
   1000k 1584k 2511k 3981k 6309k \
   10000k 15840k 25110k 39810k 63090k
do
   echo "n: $n"

   for g in $gammas; do
      for reps in $(seq 1 $iterations); do
        ../test/testrandom -x 1 -y $n -z -$g -n $n count_powerlaw >> raw$g &
      done
   done

   wait
done

echo "Reformat data and compute statistics"
for g in $gammas; do
   cut raw$g -d" " -f3,5 | ../../tools/stat.pl 0 > stats$g
   cat stats$g >> pop_count.dat
   echo -e "\n" >> pop_count.dat
done

