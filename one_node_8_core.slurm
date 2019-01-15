#!/bin/bash
#SBATCH -p physical
#SBATCH --time=00:15:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=8
#SBATCH --cpus-per-task=1
#SBATCH --mail-type=ALL
#SBATCH --mail-user=n.chang1@student.unimelb.edu.au

module load Python/2.7.11-goolf-2015a
echo "1 node 8 core"
time mpirun python distributor.py
