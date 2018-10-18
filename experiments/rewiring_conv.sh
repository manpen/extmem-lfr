#!/bin/bash

dir="rewiring_conv"
script="$dir/script"
echo > $script
iterations=50

   
for n in 1000 2150 4640 10000 21500 46400 100000; do
   for k in 2 4; do
      for r in 0 10; do
         for mu in 0.2 0.6; do
            mind=$(perl -e "print 10*$k")
            maxd=$(perl -e "print $n/20*$k")
            minc="20"
            maxc=$(perl -e "print $n/10")
            param="-n $n -i $mind -a $maxd -m $mu -j -2 -z -1 -x $minc -y $maxc -l $n -k $k -r $r -b 1Gi"

            mdir="$dir/n$n-k$k-mu$mu-r$r"
            mkdir -p $mdir

            for iter in $(seq $iterations); do
               echo "./pa_lfr $param -s $iter$n >> $mdir/log$iter 2>&1" >> $script
            done
         done
      done
   done
done

shuf $script > "$script.shuf"
settings=$(wc -l $script)

export OMP_NUM_THREADS=1
cores=$(grep processor /proc/cpuinfo | wc -l)
cores=$(perl -e "printf("%d", $cores - 2)")
echo "Generated $settings commands; execute $cores-wise parallel"
xargs --arg-file="$script.shuf" --max-procs=$cores --replace --verbose /bin/sh -c "{}"
