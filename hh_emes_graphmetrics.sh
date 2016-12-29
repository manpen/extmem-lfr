#!/bin/bash
# create parentfolder
mkdir -p hh_emes_graphmetrics

# create subfolder
now="$(date +'%d-%m-%Y')"
echo "[standard_ESTFP_graphmetrics] Date = ${now}"
count=$(find ./hh_emes_graphmetrics -type d | wc -l)
echo "[standard_ESTFP_graphmetrics] Folder Count: $((${count}-1))"
echo "[standard_ESTFP_graphmetrics] Next Folder Index: ${count}"
foldername="log${count}_${now}"
mkdir -p hh_emes_graphmetrics/${foldername}
echo "[standard_ESTFP_graphmetrics] Created Folder: hh_emes_graphmetrics/${foldername}"

#gamma=(2.0)
#mindeg=(10)
#nodes=(1000)
#divisor=(10)
gamma=(1.2 1.5 1.8 2.0)
mindeg=(5 10 20 100)
nodes=(100000 1000000 10000000)
divisor=(10 200)
for g in ${gamma[*]};
do
	for a in ${mindeg[*]};
		do 
			for n in ${nodes[*]};
                do 
                    for div in ${divisor[*]};
                        do
                            b=$(($n/$div))
                            echo num_nodes $n >> hh_emes_graphmetrics_${a}_${b}_${g}_${div}_${n}.log
                            echo min_deg $a >> hh_emes_graphmetrics_${a}_${b}_${g}_${div}_${n}.log
			                echo max_deg $b >> hh_emes_graphmetrics_${a}_${b}_${g}_${div}_${n}.log
                            echo gamma $g >> hh_emes_graphmetrics_${a}_${b}_${g}_${div}_${n}.log
                            echo divisor $div >> hh_emes_graphmetrics_${a}_${b}_${g}_${div}_${n}.log
                            echo swaps $(($n*10)) >> hh_emes_graphmetrics_${a}_${b}_${g}_${div}_${n}.log 
                            ./build/pa_edge_swaps -a $a -b $b -g $g -n $n -r $n -m $(($n*10)) >> hh_emes_graphmetrics_${a}_${b}_${g}_${div}_${n}.log
                            mv ./graph.metis hh_emes_graphmetrics_${a}_${b}_${g}_${div}_${n}.graphdata
                            #python3 hh_demo.py >> hh_emes_graphmetrics_${a}_${b}_${g}_${div}_${n}.graphdata
                        done
                done
        done
done

# move files
mv hh_emes_graphmetrics_*.log hh_emes_graphmetrics/${foldername}
mv hh_emes_graphmetrics_*.graphdata hh_emes_graphmetrics/${foldername}
echo "[standard_ESTFP_graphmetrics] Moved Log Files"
