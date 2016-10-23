#!/bin/bash

dir="rewiring_conv"
script="$dir/script"
echo > $script
iterations=2

   
for nd in 0 00; do
for nm in 100 215 464; do
   for k in 2 4; do
      for r in 0 0.5 1.0 2.0 4.0; do
         for mu in 0.2 0.6; do
            mdir="$dir/n$nn-k$k-mu$mu-r$r"
            mkdir -p $mdir

            n="$nm$nd"
            mind=$(perl -e "print 10*$k")
            maxd=$(perl -e "print $n/20*$k")
            minc="20"
            maxc=$(perl -e "print $n/10")
            param="-n $n -i $mind -a $maxd -m $mu -j -2 -z -1 -x $minc -y $maxc -l $n -k $k -r $r -b 1Gi"

            for iter in $(seq $iterations); do
               echo "./pa_lfr $param > $mdir/log$iter" >> $script
            done
         done
      done
   done
done
done

shuf $script > "$script.shuf"
settings=$(wc -l $script)
echo "Generated $settings commands"

export OMP_NUM_THREADS=1
cores=$(grep processor /proc/cpuinfo | wc -l)
xargs --arg-file="$script.shuf" --max-procs=$cores --replace --verbose /bin/sh -c "{}"
