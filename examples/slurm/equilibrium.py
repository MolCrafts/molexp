
import subprocess

# slow and monitoring needed
@tag(cache='pickle', monitoring=True)  # add the task to monitoring stack/queue
def write_script(convert:None)->None:
    subprocess.run(f"bash ../../coeff2input.sh lmp_run/peo_polymer", shell=True)
    subprocess.run(f"mv system.ff lmp_run/system.ff", shell=True)
    script = """ # PEO: eq
units real
atom_style full

dimension 3
boundary p p p

bond_style hybrid harmonic
angle_style hybrid harmonic
dihedral_style hybrid multi/harmonic charmm
special_bonds lj 0.0 0.0 0.5 coul 0.0 0.0 0.83333333

read_data init.data
include system.ff

pair_style lj/cut/coul/long 9.0 9.0
pair_modify tail yes
kspace_style pppm 1e-4

pair_coeff 1 1   0.2104000   3.0664734
pair_coeff 2 2   0.0000000   0.0000000
pair_coeff 3 3   0.1094000   3.3996695
pair_coeff 4 4   0.0157000   2.4713530
pair_coeff 5 5   0.1700000   3.0000123

pair_modify mix arithmetic

minimize 1e-4 1e-4 1000 1000
print "minimization done"

timestep 1.0

run 10000000
fix sk all shake 0.0001 20 0 m 1.008

restart 40000000 preeq.restart
thermo 10000
thermo_style custom step temp press vol density etotal
fix fprint all print 1000 "${step} ${tk} ${vl} ${dn} ${et} ${ep} ${ek}" file preeq.thermo screen no
fix 1 all npt temp 400.0 400.0 100.0 iso 1.0 1.0 1000.0
run 5000000  # 5000 ps
fix 1 all npt temp 400.0 1000.0 100.0 iso 1.0 1.0 1000.0
run 1500000  # 1500 ps
fix 1 all npt temp 1000.0 540.0 100.0 iso 1.0 1.0 1000.0
run 1500000  # 1500 ps
write_data preeq.data nocoeff
unfix fprint
print "pre eq"

fix 1 all npt temp 540.0 540.0 100.0 iso 1.0 1.0 1000.0
fix fprint all print 1000 "${step} ${tk} ${vl} ${dn} ${et} ${ep} ${ek}" file eq.thermo screen no
run 84000000
write_data eq.data nocoeff
"""
    
    with open(f'lmp_run/eq.in', 'w') as f:
        f.write(script)
    sub_script = """#!/bin/bash
#SBATCH -A snic2022-5-658
#SBATCH -n 64
#SBATCH -J peo
#SBATCH -o out
#SBATCH -t 07-00:00:00

mpprun lmp -in eq.in
"""
    with open(f'lmp_run/eq.sub', 'w') as f:
        f.write(sub_script)
    subprocess.run(['sbatch', 'eq.sub'], cwd='lmp_run')
    return None