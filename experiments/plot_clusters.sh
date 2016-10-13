#!/bin/bash
infile=../results-filtered.csv
stats=../../tools/stat.pl

cat results.csv | python3 filter_single_seed.py | sort -n > results-filtered.csv
cat overlap.csv | python3 filter_single_seed.py | sort -n > overlap-filtered.csv

rm -r plot_clusters
mkdir -p plot_clusters/data
cd plot_clusters

#Assort
for ovl in 1 2 3 4; do
for mu in 2 4 6; do
     label="assort_${ovl}_$mu"
     grep -P ", 0.$mu, [0-9]+, $ovl, .+, Assort, " ../gini.csv > data/$label.all
     for gen in Orig EM; do
        grep $gen data/$label.all | perl -pe 's/^(\d+),.+, ([01]\.\d+)\s*$/$1 $2\n/g' | sort -n > data/$label.$gen
        echo "\"$gen\"" >> data/$label.dat
        cat data/$label.$gen | $stats 0 >> data/$label.dat     
        echo >> data/$label.dat
        echo >> data/$label.dat
     done  
     
     gpargs="fnlabel=\"$label\"; NORMY=1; set title \"Mixing: \\\$\\\\mu = 0.$mu\\\$, Degree Assortativity, Overlap: \\\$\\\\nu = $ovl\\\$\"; set xrange [5e2 : 2e5]; set ylabel \"Degree Assortativity\"; NORMX=1"
     gnuplot -e "TEXBUILD=1; $gpargs" ../plot_clusters.gp & 
done
done

#Gini
for ovl in 1 2 3 4; do
for mu in 2 4 6; do
     label="gini_${ovl}_$mu"
     grep -P ", 0.$mu, [0-9]+, $ovl, .+, Gini, " ../gini.csv > data/$label.all
     for gen in Orig EM; do
         echo "\"$gen\"" >> data/$label.dat
         grep $gen data/$label.all | perl -pe 's/^(\d+),.+, (\d+), ([01]\.\d+)\s*$/$1 $2 $3\n/g' | sort -n | ../boxplot.py >> data/$label.dat
         echo >> data/$label.dat
         echo >> data/$label.dat
     done  
     
     gpargs="fnlabel=\"$label\"; $nx; set title \"Mixing: \\\$\\\\mu = 0.$mu\\\$\"; set ylabel \"Gini Coefficient\";"
     gnuplot -e "TEXBUILD=1; $gpargs" ../plot_boxplot.gp &     
done
done

for mu in 2 4 6; do
#for mu in ; do
     for clu in Louvain Infomap; do
         for metr in AR NMI; do
             label="${clu}_${metr}_$mu"
             grep -E ", 0.$mu, .+, $clu, $metr" $infile > data/$label.all
             for gen in Orig NetworKit EM; do
                 grep $gen data/$label.all | perl -pe 's/^(\d+),.+, ([01]\.\d+)\s*$/$1 $2\n/g' > data/$label.$gen
                 echo "\"$gen\"" >> data/$label.dat
                 cat data/$label.$gen | $stats 0 >> data/$label.dat
                 if [ "$gen" != "EM" ] ; then
                     echo >> data/$label.dat
                     echo >> data/$label.dat
                 fi
                 gpargs="fnlabel=\"$label\"; NORMY=1; set title \"Mixing \\\$\\\\mu = 0.$mu\\\$, Cluster: $clu\"; set ylabel \"$metr\"; set key bottom left"
                 gnuplot -e "TEXBUILD=1; $gpargs" ../plot_clusters.gp &
             done
         done
     done
     
     for ovl in 1 2 3 4; do
        label="avgcc_${ovl}_${mu}"
        grep -E ", 0.$mu, [0-9]+, $ovl, .+, AvgCC, ," $infile > data/$label.all
        for gen in Orig NetworKit EM; do
            grep $gen data/$label.all | perl -pe 's/^(\d+),.+, ([01]\.\d+)\s*$/$1 $2\n/g' > data/$label.$gen
            echo "\"$gen\"" >> data/$label.dat
            cat data/$label.$gen | $stats 0 >> data/$label.dat
            if [ "$gen" != "EM" ] ; then
                echo >> data/$label.dat
                echo >> data/$label.dat
            fi
            
            if [ "$ovl" != "1" ] ; then
                nx="set xrange [5e2 : 2e5]; NORMX=1"
            else
                nx=""
            fi
            
            gpargs="fnlabel=\"$label\"; $nx; NORMY=1; set title \"Mixing: \\\$\\\\mu = 0.$mu\\\$\"; set ylabel \"Avg. Local Clustering Coeff.\";"
            gnuplot -e "TEXBUILD=1; $gpargs" ../plot_clusters.gp &
        done
    done
    
    label="edges_$mu"
    grep -E ", 0.$mu, .+, AvgCC, ," $infile > data/$label.all
    for gen in Orig NetworKit EM; do
        grep $gen data/$label.all | perl -pe 's/^(\d+),.* (\d+), \w+, [^,]+, AvgCC,.*$/$1 $2\n/g' > data/$label.$gen
        echo "\"$gen\"" >> data/$label.dat
        cat data/$label.$gen | $stats 0 >> data/$label.dat
        if [ "$gen" != "EM" ] ; then
            echo >> data/$label.dat
            echo >> data/$label.dat
        fi
        gpargs="fnlabel=\"$label\"; set key bottom right; set title \"Mixing: \\\$\\\\mu = 0.$mu\\\$\"; set ylabel \"Edges\";"
        gnuplot -e "TEXBUILD=1; $gpargs" ../plot_clusters.gp &
    done    
done

for ovl in 2 3 4; do
for mu in 2 4 6; do
     label="nmi_${ovl}_$mu"
     grep -P ", 0.$mu, [0-9]+, $ovl, .+, ONMI, ," ../overlap-filtered.csv | grep -v "None" > data/$label.all
     for gen in Orig EM; do
         grep $gen data/$label.all | perl -pe 's/^(\d+),.+, ([01]\.\d+)\s*$/$1 $2\n/g' > data/$label.$gen
         echo "\"$gen\"" >> data/$label.dat
         cat data/$label.$gen | $stats 0 >> data/$label.dat
         if [ "$gen" != "EM" ] ; then
             echo >> data/$label.dat
             echo >> data/$label.dat
         fi
         gpargs="fnlabel=\"$label\"; NORMY=1; set title \"Mixing: \\\$\\\\mu = 0.$mu\\\$, Cluster: OSLOM, Overlap: \\\$\\\\nu = $ovl\\\$\"; set xrange [5e2 : 2e5]; set ylabel \"NMI\"; NORMX=1"
         gnuplot -e "TEXBUILD=1; $gpargs" ../plot_clusters.gp &
     done  
done
done




wait

for f in *.eps; do
    pdf=$(echo "$f" | perl -pe "s/\.eps/.pdf/g")
    echo "$f to $pdf"
    epstopdf $f --outfile=$pdf &
done

wait