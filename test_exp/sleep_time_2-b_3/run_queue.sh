#!/bin/bash
#SBATCH --output=time.out
#SBATCH --job-name=test_slurm
#SBATCH --chdir=/home/jicli594/work/molcrafts/molexp/test_exp/sleep_time_2-b_3
#SBATCH --get-user-env=L
#SBATCH --partition=slurm
#SBATCH --time=17
#SBATCH --mem=8GG
#SBATCH --cpus-per-task=32

sleep 2