#!/bin/bash
#SBATCH --partition=parallel
#SBATCH --ntasks=8
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=16000
#SBATCH --tmp=64000
#SBATCH --time=100:00:00
#SBATCH --mail-type=FAIL

dir="cluster_$SLURM_JOB_ID"
mkdir $dir
cd $dir
srun -o out_%j_%t python3 ~/extmem-graphgen/experiments/cluster_with_networkit.py
