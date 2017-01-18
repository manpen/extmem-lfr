#!/bin/bash
tabs 4

# parse ARGS
# get RUNS
for i in "$@"
do
case $i in
	-s*|--start=*)
	START="${i#*=}"
	shift
	;;
	-r*|--runs=*)
	RUNS="${i#*=}"
	shift
	;;
	-m=*|--mindeg=*)
	MINDEG="${i#*=}"
	shift
	;;
	-M=*|--maxdegfactor=*)
	MAXDEG="${i#*=}"
	shift
	;;
    -t=*|--thresholddiv=*)
    THRESHOLDDIV="${i#*=}"
    shift
    ;;
	*)

	;;
esac
done

# print RUNS
echo "[BENCHMARK] START = ${START}"
echo "[BENCHMARK] RUNS (10^RUNS NODES) = ${RUNS}"
echo "[BENCHMARK] MINDEG = ${MINDEG}"
echo "[BENCHMARK] MAXDEG-DIV-RATIO = ${MAXDEG}"
echo "[BENCHMARK] THRESHOLD-DIV = ${THRESHOLDDIV}"

# run benchmark
./build/cm_benchmark -t 3 -n 6 -r 1000 -a 10 #${START} ${RUNS} ${MINDEG} ${MAXDEG} ${THRESHOLDDIV}

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
	selfloops=$(grep -m 1 -e "self_loops" $f | perl -pe 's/\D+//g')
    multiedges=$(grep -m 1 -e "multi_edges" $f | perl -pe 's/\D+//g')
    multiedgesquant=$(grep -m 1 -e "multi_edges quantities" $f | perl -pe 's/\D+//g')
    echo "[BENCHMARK----] NODES = ${nodes}"
	echo "[BENCHMARK----] EDGES = ${edges}"
	echo "[BENCHMARK----] MIN_DEG = ${mindeg}"
	echo "[BENCHMARK----] MAX_DEG = ${maxdeg}"
	echo "[BENCHMARK----] ELAPSED TIME = ${etime}"
	echo "[BENCHMARK----] SELF_LOOPS = ${selfloops}"
    echo "[BENCHMARK----] MULTI_EDGES = ${multiedges}"
    echo "[BENCHMARK----] EDGES IN MULTI_EDGES = ${multiedgesquant}"
	# write out
    echo "$nodes $edges $etime $selfloops $multiedges $multiedgesquant" >> $dir/crc.tmp
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
	selfloops=$(grep -m 1 -e "self_loops" $f | perl -pe 's/\D+//g')
    multiedges=$(grep -m 1 -e "multi_edges" $f | perl -pe 's/\D+//g')
    multiedgesquant=$(grep -m 1 -e "multi_edges quantities" $f | perl -pe 's/\D+//g')
    echo "[BENCHMARK----] NODES = ${nodes}"
	echo "[BENCHMARK----] EDGES = ${edges}"
	echo "[BENCHMARK----] MIN_DEG = ${mindeg}"
	echo "[BENCHMARK----] MAX_DEG = ${maxdeg}"
	echo "[BENCHMARK----] ELAPSED TIME = ${etime}"
	echo "[BENCHMARK----] SELF_LOOPS = ${selfloops}"
    echo "[BENCHMARK----] MULTI_EDGES = ${multiedges}"
    echo "[BENCHMARK----] EDGES IN MULTI_EDGES = ${multiedgesquant}"
    # write out	
    echo "$nodes $edges $etime $selfloops $multiedges $multiedgesquant" >> $dir/r.tmp
done

sort -n $dir/r.tmp > $dir/runtime_cm_r.dat

for f in $dir/cm_tupr*.log; 

do
	echo "[BENCHMARK] Processing ${f}"
	nodes=$(grep -m 1 -e "nodes set to" $f | perl -pe 's/\D+//g')
	edges=$(grep -m 1 -e "edges set to" $f | perl -pe 's/\D+//g')
	mindeg=$(grep -m 1 -e "min_deg set to" $f | perl -pe 's/\D+//g')
	maxdeg=$(grep -m 1 -e "max_deg set to" $f | perl -pe 's/\D+//g')
	etime=$(grep -m 1 -e " Time since the last reset" $f | perl -pe 's/.* (\d+\.\d+) .*/$1/g')
	selfloops=$(grep -m 1 -e "self_loops" $f | perl -pe 's/\D+//g')
    multiedges=$(grep -m 1 -e "multi_edges" $f | perl -pe 's/\D+//g')
    multiedgesquant=$(grep -m 1 -e "multi_edges quantities" $f | perl -pe 's/\D+//g')
    echo "[BENCHMARK----] NODES = ${nodes}"
	echo "[BENCHMARK----] EDGES = ${edges}"
	echo "[BENCHMARK----] MIN_DEG = ${mindeg}"
	echo "[BENCHMARK----] MAX_DEG = ${maxdeg}"
	echo "[BENCHMARK----] ELAPSED TIME = ${etime}"
	echo "[BENCHMARK----] SELF_LOOPS = ${selfloops}"
    echo "[BENCHMARK----] MULTI_EDGES = ${multiedges}"
    echo "[BENCHMARK----] EDGES IN MULTI_EDGES = ${multiedgesquant}"
    # write out	
    echo "$nodes $edges $etime $selfloops $multiedges $multiedgesquant" >> $dir/tupr.tmp
done

sort -n $dir/tupr.tmp > $dir/runtime_cm_tupr.dat

cd $dir

gnuplot -e "MIN_DEG = 10; MAX_DEG_RATIO = 10; THRESHOLD_DIV = 10" ../../cm_benchmark_multiplot.gp
#gnuplot -e "MIN_DEG = ${MINDEG}; MAX_DEG_RATIO = ${MAXDEG}; THRESHOLD_DIV = ${THRESHOLDDIV}" ../../cm_benchmark_multiplot.gp
