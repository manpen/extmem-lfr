#!/usr/bin/env bash


echo "Simulate"

dir="exp_pop_count"
gammas="1 2 3"
iterations=8

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
   cut stats$g >> pop_count.dat
   echo -e "\n" >> pop_count.dat
done







exit

for x in 1 2 3 4 5 6 7 8 9 10; do
for y in 1k 10k 100k 1m 10m 100m; do

for n in 1k 2k 3k 4k 5k 6k 7k 8k 9k \
         10k 20k 30k 40k 50k 60k 70k 80k 90k \
         100k 200k 300k 400k 500k 600k 700k 800k 900k \
         1m 2m 3m 4m 5m 6m 7m 8m 9m \
         10m 20m 30m 40m 50m 60m 70m 80m 90m \
         100m 200m 300m 400m 500m 600m 700m 800m 900m;
do

  echo "test/testrandom -x $x -y $y -z -2 -n $n >> pop_count_$suf"

  #test/testrandom -x $x -y $y -z -2 -n $n count_powerlaw >> pop_count_$suf


done
done
done

cat pop_count* > tmp_pop_count
sort -n -s -k 3 tmp_pop_count > stmp_pop_count


for x in 1 2 3 4 5 6 7 8 9 10; do
   rm gpop_count_$x

for y in 1000 10000 100000 1000000 10000000 100000000; do
   grep -h -e "^$x $y " stmp_pop_count >> gpop_count_$x
   echo >> gpop_count_$x
   echo >> gpop_count_$x
done


done