#/bin/bash
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

gamma=(2.0)
t=(5 10 20 1000000)

for t in ${t[*]};
do
    for g in ${gamma[*]};
    do

        $(touch crc_${g}_${t}.dat)
        $(touch rnd_${g}_${t}.dat)

        for i in `seq 1 $RUNS`;
        do
            echo $i
            # run
            ./build/cm_benchmark -g ${g} -t 4 -n 5 -r 200 -a 10 -x ${t}
         
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

                $(echo -e "$nodes\t$edges\t$etime\t$selfloops\t$multiedges\t$multiedgesquant" >> crc_${g}_${t}.dat)
                suffix=.log
                nf=${f%$suffix}
                $(mv $f ${nf}_${g}_${t}_${i}.pro) 
            done
            #for f in $dir/cm_crc*.pro;
            #do
            #    suffix=.pro
            #    newdatafilenameprefix=${f%$suffix}          
            #    newdatafilename=${newdatafilenameprefix}_${g}_${t}.ana
            #    $(mv $f $newdatafilename)
            #done
         #   for f in $dir/cm_tupr*.log;
        #    do
  #              nodes=$(grep -m 1 -e "nodes set to" $f | perl -pe 's/\D+//g')
       #         edges=$(grep -m 1 -e "edges set to" $f | perl -pe 's/\D+//g')
      #          mindeg=$(grep -m 1 -e "min_deg set to" $f | perl -pe 's/\D+//g')
     #           maxdeg=$(grep -m 1 -e "max_deg set to" $f | perl -pe 's/\D+//g')
    #            etime=$(grep -m 1 -e " Time since the last reset" $f | perl -pe 's/.* (\d+\.\d+) .*/$1/g')
   #             selfloops=$(grep -m 1 -e "self_loops" $f | perl -pe 's/\D+//g')
  #              multiedges=$(grep -m 1 -e "multi_edges" $f | perl -pe 's/\D+//g')
 #               multiedgesquant=$(grep -m 1 -e "multi_edges quantities" $f | perl -pe 's/\D+//g')
#
            #    $(echo -e "$nodes\t$edges\t$etime\t$selfloops\t$multiedges\t$multiedgesquant" >> rnd.dat)
            #done

        done
    done
done

#for t in ${t[*]};
#do
#    for g in ${gamma[*]};
#    do
#        #$(sort -n crc_${g}_${t}.dat)
#        #$(sort -n rnd_${g}_${t}.dat)
#    done
#done

mv crc_*.dat $dir
mv rnd_*.dat $dir
