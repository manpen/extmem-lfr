#!/bin/bash
#SBATCH --partition=parallel
#BATCH --partition=test
#SBATCH --constraint=intel20
#SBATCH --ntasks=4
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=1000
#BATCH --tmp=64000
#SBATCH --time=100:00:00
#SBATCH --mail-type=FAIL

scriptdir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
dir="/scratch/memhierarchy/penschuck/networks_$SLURM_JOB_ID/"
mkdir $dir
#cd $dir
#export OMP_NUM_THREADS=8
#echo "disk=,24Gi,memory" > .stxxl
srun -o out_%j_%t ./gen_only.py -n

