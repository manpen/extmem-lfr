#!/bin/bash

# create parentfolder
mkdir -p convergence_tests

# create subfolder
now="$(date +'%d-%m-%Y')"
echo "[CONVERGENCE] Date = ${now}"
count=$(find ./convergence_tests -type d | wc -l)
echo "[CONVERGENCE] Folder Count: $((${count}-1))"
echo "[CONVERGENCE] Next Folder Index: ${count}"
foldername="log${count}_${now}"
mkdir -p convergence_tests/${foldername}
echo "[CONVERGENCE] Created Folder: convergence_tests/${foldername}"

#gamma=(2.0)
#mindeg=(10)
#nodes=(1000)
#divisor=(10)
gamma=(1.5 1.8 2.0)
mindeg=(5 10 20 100)
nodes=(100000 1000000)
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
                            echo num_nodes $n >> hh_cm_emes_${a}_${b}_${g}_${div}_${n}.log
                            echo min_deg $a >> hh_cm_emes_${a}_${b}_${g}_${div}_${n}.log
			                echo max_deg $b >> hh_cm_emes_${a}_${b}_${g}_${div}_${n}.log
                            echo gamma $g >> hh_cm_emes_${a}_${b}_${g}_${div}_${n}.log
                            echo divisor $div >> hh_cm_emes_${a}_${b}_${g}_${div}_${n}.log
                            ./build/cm_emtfp_convergence -a $a -b $b -g $g -n $n >> hh_cm_emes_${a}_${b}_${g}_${div}_${n}.log
                            mv ./graph.metis hh_cm_emes_${a}_${b}_${g}_${div}_${n}.graphdata
                            #python3 hh_demo.py >> hh_cm_emes_${a}_${b}_${g}_${div}_${n}.graphdata
                        done
                done
        done
done

# move files
mv hh_cm_emes_*.log convergence_tests/${foldername}
mv hh_cm_emes_*.graphdata convergence_tests/${foldername}
echo "[CONVERGENCE] Moved Log Files"
