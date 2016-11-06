#!/bin/bash
tabs 4

# parse ARGS
# get RUNS
for i in "$@"
do
case $i in
	-r=*|--runs=*)
	RUNS="${i#*=}"
	;;
	*)

	;;
esac
done

# print RUNS
echo "[BENCHMARK] RUNS = ${RUNS}"

# run benchmark
./build/cm_benchmark ${RUNS}

# create parentfolder
mkdir -p measurements

# create subfolder
now="$(date +'%d-%m-%Y')"
echo "[BENCHMARK] DATE = ${now}"
count=$(find ./measurements -type d | wc -l)
echo "[BENCHMARK] FOLDER COUNT = $((${count}-1))"
echo "[BENCHMARK] NEXT INDEX = ${count}"
foldername="log${count}_${now}"
mkdir -p measurements/${foldername}
echo "[BENCHMARK] CREATED FOLDER = measurements/${foldername}"

# move files
mv cm_*.log measurements/${foldername}
echo "[BENCHMARK] MOVED log files"

dir=measurements/${foldername}

for f in $dir/cm_crc*.log; 
do
	echo "[BENCHMARK] Processing ${f}"
	nodes=$(grep -m 1 -e "nodes set to" $f | perl -pe 's/\D+//g')
	edges=$(grep -m 1 -e "edges set to" $f | perl -pe 's/\D+//g')
	mindeg=$(grep -m 1 -e "min_deg set to" $f | perl -pe 's/\D+//g')
	maxdeg=$(grep -m 1 -e "max_deg set to" $f | perl -pe 's/\D+//g')
	etime=$(grep -m 1 -e " Time since the last reset" $f | perl -pe 's/.* (\d+\.\d+) .*/$1/g')
	echo "[BENCHMARK----] NODES = ${nodes}"
	echo "[BENCHMARK----] EDGES = ${edges}"
	echo "[BENCHMARK----] MIN_DEG = ${mindeg}"
	echo "[BENCHMARK----] MAX_DEG = ${maxdeg}"
	echo "[BENCHMARK----] ELAPSED TIME = ${etime}"
	# write out
    echo "$nodes $edges $etime" >> $dir/crc.tmp
done

sort -n $dir/crc.tmp > $dir/runtime_cm_crc.dat

for f in $dir/cm_r*.log; 

do
	echo "[BENCHMARK] Processing ${f}"
	nodes=$(grep -m 1 -e "nodes set to" $f | perl -pe 's/\D+//g')
	edges=$(grep -m 1 -e "edges set to" $f | perl -pe 's/\D+//g')
	mindeg=$(grep -m 1 -e "min_deg set to" $f | perl -pe 's/\D+//g')
	maxdeg=$(grep -m 1 -e "max_deg set to" $f | perl -pe 's/\D+//g')
	etime=$(grep -m 1 -e " Time since the last reset" $f | perl -pe 's/.* (\d+\.\d+) .*/$1/g')
	echo "[BENCHMARK----] NODES = ${nodes}"
	echo "[BENCHMARK----] EDGES = ${edges}"
	echo "[BENCHMARK----] MIN_DEG = ${mindeg}"
	echo "[BENCHMARK----] MAX_DEG = ${maxdeg}"
	echo "[BENCHMARK----] ELAPSED TIME = ${etime}"
	# write out	
    echo "$nodes $edges $etime" >> $dir/r.tmp
done

sort -n $dir/r.tmp > $dir/runtime_cm_r.dat

cd $dir

gnuplot ../../cm_benchmark_multiplot.gp