#!/bin/bash
#SBATCH --job-name=NMx4
#SBATCH -A naiss2023-1-37
#SBATCH -n 32
#SBATCH -o out
#SBATCH -e err
#SBATCH -t 07-00:00:00

mpprun ~/.local/bin/lmp -in eq.in