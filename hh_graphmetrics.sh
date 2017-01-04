#!/bin/bash

# runs per (mindeg, maxdeg, gamma, n)-tuple
RUNS=5

# create parentfolder
mkdir -p hh_graphmetrics

# create subfolder
now="$(date +'%d-%m-%Y')"
echo "[HHGRAPHMETRICS] Date = ${now}"
count=$(find ./hh_graphmetrics -type d | wc -l)
echo "[HHGRAPHMETRICS] Folder Count: $((${count}-1))"
echo "[HHGRAPHMETRICS] Next Folder Index: ${count}"
foldername="log${count}_${now}"
mkdir -p hh_graphmetrics/${foldername}
echo "[HHGRAPHMETRICS] Created Folder: hh_graphmetrics/${foldername}"

#gamma=(2.0)
#mindeg=(10)
#nodes=(10000)
#divisor=(10)
gamma=(1.5 2.0)
mindeg=(5 10)
nodes=(10000 50000 150000)
divisor=(10 200)
for g in ${gamma[*]};
do
	for a in ${mindeg[*]};
		do 
			for n in ${nodes[*]};
                do 
                    for div in ${divisor[*]};
                        do
                            for j in `seq 1 $RUNS`;
                            do
                                echo "Doing iteration ${j}"
                                b=$(($n/$div))
                                echo num_nodes $n >> hh_graphmetrics_${a}_${b}_${g}_${div}_${n}_${j}.log
                                echo min_deg $a >> hh_graphmetrics_${a}_${b}_${g}_${div}_${n}_${j}.log
                                echo max_deg $b >> hh_graphmetrics_${a}_${b}_${g}_${div}_${n}_${j}.log
                                echo gamma $g >> hh_graphmetrics_${a}_${b}_${g}_${div}_${n}_${j}.log
                                echo divisor $div >> hh_graphmetrics_${a}_${b}_${g}_${div}_${n}_${j}.log
                                ./build/hh_graphmetrics -a $a -b $b -g $g -n $n -j $j >> hh_graphmetrics_${a}_${b}_${g}_${div}_${n}_${j}.log
                                mv ./graph.metis hh_graphmetrics_${a}_${b}_${g}_${div}_${n}_${j}.graphdata
                                #python3 hh_demo.py >> hh_graphmetrics_${a}_${b}_${g}_${div}_${n}_${j}.graphdata
                            done
                            echo "Generating graphmetric file"
                            $(./graph_analyze.sh -a=${a} -b=${b} -g=${g} -d=${div} -n=${n})
                            echo "Removing graphdata file"
                            rm *_${a}_${b}_${g}_${div}_${n}*.graphdata
                        done
                done
        done
done

# move files
mv hh_graphmetrics_*.log hh_graphmetrics/${foldername}
mv sorted_metrics_*.dat hh_graphmetrics/${foldername}
echo "[HHGRAPHMETRICS] Moved Log Files"
