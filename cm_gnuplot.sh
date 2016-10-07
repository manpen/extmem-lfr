#!/bin/bash
tabs 4

function process {
    file=$1
    rfile=$2
    params=(${file//_/ })
    dmin=${params[1]}
    dmax=${params[2]}
    # CRC Tables
    count=0
    crc_sl=0
    for i in $( awk '{print $1; }' $1 ) ; do
        crc_sl=$(echo $crc_sl+$i | bc)
        ((count++))
    done
    crc_sl=$(echo $crc_sl / $count | bc)
    count=0
    crc_ms=0
    for j in $( awk '{print $2; }' $1 ) ; do
        crc_ms=$(echo $crc_ms+$j | bc)
        ((count++))
    done
    crc_ms=$(echo $crc_ms / $count | bc)
    count=0
    crc_mm=0
    for k in $( awk '{print $3; }' $1 ) ; do
        crc_mm=$(echo $crc_mm+$k | bc)
        ((count++))
    done
    crc_mm=$(echo $crc_mm / $count | bc)
    # R64 Tables
    count=0
    r64_sl=0
    for i in $( awk '{print $1; }' $2 ) ; do
        r64_sl=$(echo $r64_sl+$i | bc)
        ((count++))
    done
    r64_sl=$(echo $r64_sl / $count | bc)
    count=0
    r64_ms=0
    for j in $( awk '{print $2; }' $2 ) ; do
        r64_ms=$(echo $r64_ms+$j | bc)
        ((count++))
    done
    r64_ms=$(echo $r64_ms / $count | bc)
    count=0
    r64_mm=0
    for k in $( awk '{print $3; }' $2 ) ; do
        r64_mm=$(echo $r64_mm+$k | bc)
        ((count++))
    done
    r64_mm=$(echo $r64_mm / $count | bc)

    echo -e "\t\t\tCRC,SL:\t$crc_sl"
    echo -e "\t\t\tCRC,MS:\t$crc_ms"
    echo -e "\t\t\tCRC,MM:\t$crc_mm"
    echo -e "\t\t\tR64,SL:\t$r64_sl"
    echo -e "\t\t\tR64,MS:\t$r64_ms"
    echo -e "\t\t\tR64,MM:\t$r64_mm"
}

echo "Beginning data analysis and processing..."

echo -e "\tProcessing CRC files..."
for file in ./build/CM_[[:digit:]]*.dat ; do
    rfile=./build/CM_r${file:10}
    echo -e "\t\tProcessing ${file:8} and ${rfile:8}..."
    process $file $rfile
done
