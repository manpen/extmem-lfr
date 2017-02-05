#!/bin/bash

RUNS=5

SNAPS=0
FREQUENCY=10
EDGESCANS=1
for i in "$@"
do
case $i in
    -s*|--snaps=*)
    SNAPS="${i#*=}"
    shift
    ;;
    -f*|--freq=*)
    FREQUENCY="${i#*=}"
    shift
    ;;
    -e*|--scans=*)
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

gamma=(1.5 2.0)
mindeg=(5 10)
nodes=(10000 25000)
divisor=(10 200)
#gamma=(1.5 2.0)
#mindeg=(5 10)
#nodes=(10000 50000 150000)
#divisor=(10 200)
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
                                echo num_nodes $n >> hh_cm_memes_emes_graphmetrics_${a}_${b}_${g}_${div}_${n}_${j}.log
                                echo min_deg $a >> hh_cm_memes_emes_graphmetrics_${a}_${b}_${g}_${div}_${n}_${j}.log
                                echo max_deg $b >> hh_cm_memes_emes_graphmetrics_${a}_${b}_${g}_${div}_${n}_${j}.log
                                echo gamma $g >> hh_cm_memes_emes_graphmetrics_${a}_${b}_${g}_${div}_${n}_${j}.log
                                echo divisor $div >> hh_cm_memes_emes_graphmetrics_${a}_${b}_${g}_${div}_${n}_${j}.log
                                echo swaps $(($n*10)) >> hh_cm_memes_emes_graphmetrics_${a}_${b}_${g}_${div}_${n}_${j}.log 
                                if [ $SNAPS -eq 1 ]
                                then
                                    ./build/memtfp_combined_benchmark -a $a -b $b -g $g -n $n -e TFP -w $EDGESCANS -z -f $FREQUENCY >> hh_cm_memes_emes_graphmetrics_${a}_${b}_${g}_${div}_${n}_${j}.log
                                    echo "Processing Snapshots"
                                    count=$(($(find ./graph_snapshot_*.metis | wc -l) -1))
                                    echo "Snapshotcount w/o initial: $count"
                                    python3 ./graph_generic_networkit.py graph_snapshot_init.metis >> tmp_snapshot_0.graphanalyze
                                    snapfile=hh_emes_graphmetrics_${a}_${b}_${g}_${div}_${n}_${j}.degass
                                    ccsnapfile=hh_emes_graphmetrics_${a}_${b}_${g}_${div}_${n}_${j}.ccoeff
                                    echo "Degree Assortativity Datafile: $snapfile"
                                    echo "Clustering Coefficient Datafile: $ccsnapefile"
                                    $(echo -e "# Round \t Degree_Assortativity" >> $snapfile)
                                    $(echo -e "# Round \t Clustering_Coefficient" >> $ccsnapfile)
                                    if [ "$count" -gt "0" ]
                                    then
                                        for k in `seq 1 $count`;
                                        do
                                            filename=graph_snapshot_${k}.metis
                                            echo "Processing $filename"
                                            python3 ./graph_generic_networkit.py $filename >> tmp_snapshot_${k}.graphanalyze
                                        done
                                        for z in `seq 0 $count`;
                                        do
                                            echo $z
                                            # Make snapfile
                                            # Get Degree Assortativity
                                            while IFS=$'\t' read -r column1 column2 ;
                                            do
                                                case $column1 in
                                                    "degree assortativity")
                                                        p_da=$column2
                                                        shift
                                                        ;;
                                                    "clustering coefficient")
                                                        p_cc=$column2
                                                        shift
                                                        ;;
                                                    *)
                                                        ;;
                                                esac
                                            done < tmp_snapshot_${z}.graphanalyze
                                            # Write out
                                            echo "Current Degree Assortativity: " $p_da
                                            echo "Current Clustering Coefficient: " $p_cc
					    $(echo -e "$((${z}*${FREQUENCY})) \t $p_da" >> $snapfile)
					    $(echo -e "$((${z}*${FREQUENCY})) \t $p_cc" >> $ccsnapfile)
                                        done
                                     fi    
                                     # Remove snap files
                                     rm graph_snapshot_*.metis
                                     # Remove tmp snap files
                                     # rm tmp_snapshot_*.graphanalyze
                                else
                                    ./build/memtfp_combined_benchmark -a $a -b $b -g $g -n $n -e TFP -w $EDGESCANS >> hh_cm_memes_emes_graphmetrics_${a}_${b}_${g}_${div}_${n}_${j}.log
                                fi
                                mv ./graph.metis hh_cm_memes_emes_graphmetrics_${a}_${b}_${g}_${div}_${n}_${j}.graphdata
                            done
                            echo "Generating graphmetric file"
                            $(./graph_analyze.sh -f=hh_cm_memes_emes_graphmetrics -a=${a} -b=${b} -g=${g} -d=${div} -n=${n})
                            echo "Removing graphdata file"
                            rm *_${a}_${b}_${g}_${div}_${n}*.graphdata
                       done
                done
        done
done

# move files
mv hh_cm_memes_emes_graphmetrics_*.log hh_cm_memes_emes_graphmetrics/${foldername}
mv hh_cm_memes_emes_graphmetrics_*.degass hh_cm_memes_emes_graphmetrics/${foldername}
mv hh_cm_memes_emes_graphmetrics_*.ccoeff hh_cm_memes_emes_graphmetrics/${foldername}
mv sorted_metrics_*.dat hh_cm_memes_emes_graphmetrics/${foldername}
echo "[combined_ESTFP_graphmetrics] Moved Log Files"
