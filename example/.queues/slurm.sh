#!/bin/bash
#SBATCH --job-name={{job_name}}
#SBATCH -A {{account}}
#SBATCH -n {{ncores}}
#SBATCH -o out
#SBATCH -e err
#SBATCH -t 07-00:00:00

{{command}}