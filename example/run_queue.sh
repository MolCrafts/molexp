#!/bin/bash
#SBATCH --output=time.out
#SBATCH --job-name=['N', 'M']x1
#SBATCH --chdir=.
#SBATCH --get-user-env=L
#SBATCH --partition=slurm
#SBATCH --time=17
#SBATCH --cpus-per-task=32

mpprun ~/.local/bin/lmp -in eq.in