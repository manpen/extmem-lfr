#!/bin/bash

# parse args for multiple of m edges
EDGESCANS=1
for i in "$@"
do
    case $i in
        -s*|--scans=*)
        EDGESCANS="${i#*=}"
        shift
        ;;
    *)
        ;;
esac
done

# create parentfolder
mkdir -p hh_cm_memes_emes_graphmetrics

# create subfolder
now="$(date +'%d-%m-%Y')"
echo "[combined_ESTFP_graphmetrics] Date = ${now}"
count=$(find ./hh_cm_memes_emes_graphmetrics -type d | wc -l)
echo "[combined_ESTFP_graphmetrics] Folder Count: $((${count}-1))"
echo "[combined_ESTFP_graphmetrics] Next Folder Index: ${count}"
foldername="log${count}_${now}"
mkdir -p hh_cm_memes_emes_graphmetrics/${foldername}
echo "[combined_ESTFP_graphmetrics] Created Folder: hh_cm_memes_emes_graphmetrics/${foldername}"

gamma=(2.0)
mindeg=(10)
nodes=(1000)
divisor=(10)
#gamma=(1.2 1.5 1.8 2.0)
#mindeg=(5 10 20 100)
#nodes=(100000 1000000 10000000 100000000)
#divisor=(10 200)
for g in ${gamma[*]};
do
	for a in ${mindeg[*]};
		do 
			for n in ${nodes[*]};
                do 
                    for div in ${divisor[*]};
                        do
                            b=$(($n/$div))
                            echo num_nodes $n >> hh_cm_memes_emes_graphmetrics_${a}_${b}_${g}_${div}_${n}.log
                            echo min_deg $a >> hh_cm_memes_emes_graphmetrics_${a}_${b}_${g}_${div}_${n}.log
			                echo max_deg $b >> hh_cm_memes_emes_graphmetrics_${a}_${b}_${g}_${div}_${n}.log
                            echo gamma $g >> hh_cm_memes_emes_graphmetrics_${a}_${b}_${g}_${div}_${n}.log
                            echo divisor $div >> hh_cm_memes_emes_graphmetrics_${a}_${b}_${g}_${div}_${n}.log
                            echo swaps $(($n*10)) >> hh_cm_memes_emes_graphmetrics_${a}_${b}_${g}_${div}_${n}.log 
                            ./build/memtfp_combined_benchmark -a $a -b $b -g $g -n $n -r $n -m $(($n*$EDGESCANS)) >> hh_cm_memes_emes_graphmetrics_${a}_${b}_${g}_${div}_${n}.log
                            python3 hh_demo.py >> hh_cm_memes_emes_graphmetrics_${a}_${b}_${g}_${div}_${n}.graphdata
                        done
                done
        done
done

# move files
mv hh_cm_memes_emes_graphmetrics_*.log hh_cm_memes_emes_graphmetrics/${foldername}
mv hh_cm_memes_emes_graphmetrics_*.graphdata hh_cm_memes_emes_graphmetrics/${foldername}
echo "[combined_ESTFP_graphmetrics] Moved Log Files"
