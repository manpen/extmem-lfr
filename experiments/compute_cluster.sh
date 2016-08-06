#!/bin/bash
#BATCH --partition=parallel
#SBATCH --partition=test
#BATCH --constraint=intel20
#SBATCH --ntasks=16
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=16000
#SBATCH --tmp=64000
#SBATCH --time=1:00:00
#SBATCH --mail-type=FAIL

srun -o out_%j_%t python3 ~/extmem-graphgen/experiments/compute_cluster.py
