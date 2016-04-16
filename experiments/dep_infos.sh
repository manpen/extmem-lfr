#!/usr/bin/env bash

dir="dep_infos"
iterations=9
#rs="0.1 0.2 0.5 1"
rs="0.1 0.5 1 1.25"

mkdir $dir

echo "Simulation"
for reps in $(seq 2 $iterations); do
    for x in $rs ; do
        echo "x: $x, round: $reps"
        ./pa_edge_swaps -n 10m -a 18 -b 5000 -e TFP -x $x -y 1 > $dir/run${x}_${reps}.orig
    done
done

echo "Analyse"
cd $dir

rm dep_infos_*.dat
for x in $rs ; do
    rm spe_$x.raw ss_$x.raw epe_$x.raw
    for f in run${x}_*.orig; do
        nswaps=$(grep "Set numSwaps =" $f | perl -pe 's/\D+//g')
        grep "#SWAPS-PER-EDGE-ID" $f | perl -pe "s/\\s*#.+$/ $nswaps/" >> spe_$x.raw
        grep "#STATE-SIZE" $f | perl -pe "s/\\s*#.+$/ $nswaps/" >> ss_$x.raw
        grep "#EXIST-REQ-PER-EDGE" $f | perl -pe "s/\\s*#.+$/ $nswaps/" >> epe_$x.raw
    done
    
    for t in spe ss epe; do
        cat ${t}_$x.raw | \
            sort -n | \
            perl -ne '/(\d+) (\d+) (\d+)/; printf("$1 $2 $3 %.9f\n", $2/$3);' | \
            ../../tools/stat.pl 0 > ${t}_$x.stat
            
        echo "\"\$r/n = $x\$\"" >> dep_infos_$t.dat
        cat ${t}_$x.stat >> dep_infos_$t.dat
        echo -e '\n'>> dep_infos_$t.dat
    done
done

