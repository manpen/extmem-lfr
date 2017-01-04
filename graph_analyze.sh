#!/bin/bash

for i in "$@"
do
case $i in
    -a*)
        a="${i#*=}"
        shift
        ;;
    -b*)
        b="${i#*=}"
        shift
        ;;
    -g*)
        g="${i#*=}"
        shift
        ;;
    -d*)
        d="${i#*=}"
        shift
        ;;
    -n*)
        n="${i#*=}"
        shift
        ;;
    *)

        ;;
esac
done

echo a = $a
echo b = $b
echo g = $g
echo d = $d
echo n = $n

echo Calculating graphmetrics w/ networkit...

datafile=metrics_${a}_${b}_${g}_${d}_${n}.dat

rm -f $datafile
rm -f sorted_$datafile

for f in ./*_${a}_${b}_${g}_${d}_${n}*.graphdata
do
    index="$(echo "$f" | sed 's/.*'${a}_${b}_${g}_${d}_${n}'_\([0-9]\|[0-9][0-9]\).graphdata$/\1/')"
    rm -f tmp_${index}.graphanalyze
    python3 ./graph_generic_networkit.py $f >> tmp_${index}.graphanalyze
    #p_cc="$(cat tmp_${index}.graphanalyze | sed 's/.*clustering coefficient[ \t][ \t]\([0-1][.][0-9]*\).*/\1/')"
    #echo Clustering coefficient: $p_cc
    while IFS=$'\t' read -r column1 column2 ; 
    do
        case $column1 in
            "nodes, edges")
                IFS=', ' read -ra nodesedges <<< $column2
                p_nodes=${nodesedges[0]}
                p_edges=${nodesedges[1]}
                shift
                ;;
            "clustering coefficient")
                p_cc=$column2
                shift
                ;;
            "degree assortativity")
                p_da=$column2
                shift
                ;;
            "density")
                p_de=$column2
                shift
                ;;
            *)

                ;;
        esac
    done < tmp_${index}.graphanalyze
    echo Clustering coefficient: $p_cc
    echo Degree Assortativity: $p_da
    echo Density: $p_de
    echo Nodes: $p_nodes
    echo Edges: $p_edges
    $(echo -e "$index \t $p_cc \t $p_da \t $p_de \t $p_nodes \t $p_edges" >> $datafile)
done

(echo -e "#Index \t Clustering_Coefficient \t Degree_Assortativity \t Density \t Nodes \t Edges" >> $datafile)
sort -n $datafile >> sorted_$datafile

rm $datafile
rm tmp_*.graphanalyze
