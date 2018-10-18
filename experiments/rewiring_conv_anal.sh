#!/bin/bash
dir="rewiring_conv"
rm -r $dir
mkdir -p $dir

for d in rewiring_conv_input/n*; do
    key=$(basename "$d")
    grep -h "# ComRewStats" $d/log* | perl -pe 's/# Com.*$//' | sort -n > $dir/$key.txt
    ./rewiring_conv_rate.py $dir/$key.txt > $dir/conv-$key.txt 
    
    n=$(echo $key    | perl -pe 's/n(\d+)-(.*)$/$1/')
    skey=$(echo $key | perl -pe 's/n(\d+)-(.*)$/$2/')
    
    grep "below" $dir/conv-$key.txt | perl -pe "s/^.+:(.+)/$n \$1/" >> "$dir/glob-$skey.tmp"
done

wait

cd $dir
for x in conv*-r0.txt; do
    key=$(echo $x | perl -pe 's/conv-n(\d+)-k(\d)-mu0\.(\d).*/n$1-k$2-mu0.$3/')
    gnuplot -e "key='$key'" ../rewiring_conv_local.gp &
done

wait

for r in "0" "4.0"; do
    for g in glob*-r$r.tmp; do
        skey=$(echo $g | perl -pe "s/glob-(.*)-r$r.tmp/\1/")
        echo "gnuplot -e 'fnlabel=\"$skey\"' ../rewiring_conv_global.gp &" >> glob-params.tmp
        echo "'r=$r'" >> glob-$skey.dat
        cat $g | sort -n >> glob-$skey.dat       
        echo >> glob-$skey.dat 
        echo >> glob-$skey.dat 
    done
    
    cat glob-params.tmp | sort | uniq > glob.params.uniq
    bash ./glob.params.un
done
    

