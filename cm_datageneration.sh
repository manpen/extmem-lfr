#!/bin/bash
tabs 4

RUNS=5

# create parentfolder
mkdir -p cm_benchmark

# create subfolder
now="$(date +'%d-%m-%Y')"
count=$(find ./cm_benchmark -type d | wc -l)
foldername="log${count}_${now}"
mkdir -p cm_benchmark/${foldername}

dir=cm_benchmark/${foldername}

gamma=(2.0 2.5 3.0)

for g in ${gamma[*]};
do

    $(touch crc_${g}.dat)
    $(touch rnd_${g}.dat)

    for i in `seq 1 $RUNS`;
    do
        echo $i
        # run
        ./build/cm_benchmark -g ${g} -t 3 -n 6 -r 200 -a 10 -x 5
     
        mv cm_*.log $dir 
       
        for f in $dir/cm_crc*.log;
        do
            nodes=$(grep -m 1 -e "nodes set to" $f | perl -pe 's/\D+//g')
            edges=$(grep -m 1 -e "edges set to" $f | perl -pe 's/\D+//g')
            mindeg=$(grep -m 1 -e "min_deg set to" $f | perl -pe 's/\D+//g')
            maxdeg=$(grep -m 1 -e "max_deg set to" $f | perl -pe 's/\D+//g')
            etime=$(grep -m 1 -e " Time since the last reset" $f | perl -pe 's/.* (\d+\.\d+) .*/$1/g')
            selfloops=$(grep -m 1 -e "self_loops" $f | perl -pe 's/\D+//g')
            multiedges=$(grep -m 1 -e "multi_edges" $f | perl -pe 's/\D+//g')
            multiedgesquant=$(grep -m 1 -e "multi_edges quantities" $f | perl -pe 's/\D+//g')

            $(echo -e "$nodes\t$edges\t$etime\t$selfloops\t$multiedges\t$multiedgesquant" >> crc.dat)
        done

        for f in $dir/cm_tupr*.log;
        do
            nodes=$(grep -m 1 -e "nodes set to" $f | perl -pe 's/\D+//g')
            edges=$(grep -m 1 -e "edges set to" $f | perl -pe 's/\D+//g')
            mindeg=$(grep -m 1 -e "min_deg set to" $f | perl -pe 's/\D+//g')
            maxdeg=$(grep -m 1 -e "max_deg set to" $f | perl -pe 's/\D+//g')
            etime=$(grep -m 1 -e " Time since the last reset" $f | perl -pe 's/.* (\d+\.\d+) .*/$1/g')
            selfloops=$(grep -m 1 -e "self_loops" $f | perl -pe 's/\D+//g')
            multiedges=$(grep -m 1 -e "multi_edges" $f | perl -pe 's/\D+//g')
            multiedgesquant=$(grep -m 1 -e "multi_edges quantities" $f | perl -pe 's/\D+//g')

            $(echo -e "$nodes\t$edges\t$etime\t$selfloops\t$multiedges\t$multiedgesquant" >> rnd.dat)
        done

        cd $dir
        mkdir $i
        mv cm_*.log $i
        cd ../..
    done
done

for g in ${gamma[*]};
do
    $(sort -n crc_${g}.dat)
    $(sort -n rnd_${g}.dat)
done

mv crc_*.dat $dir
mv rnd_*.dat $dir
