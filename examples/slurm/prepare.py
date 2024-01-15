from functools import cache
import numpy as np
import os
import nglview as ngl
from ase.io import read, write
from molmass import Formula
import random
import subprocess
from hamilton.function_modifiers import tag

# fast and resourse free task
@tag(cache='pickle')
def assemble(param:dict)->dict:
    from pathlib import Path
    import random
    # units_path = Path('./units')
    # units_path.mkdir(parents=True, exist_ok=True)
    tmp_path = Path('./tmp')
    tmp_path.mkdir(parents=True, exist_ok=True)
    seed = random.randint(0, 1000)
    input = f"""
    tolerance 2.0
    output peo20.pdb
    filetype pdb
    seed {seed}
    structure {root}/peo_polymer.pdb
    number {chain}
    inside cube 0. 0. 0. 80.
    end structure
    """
    with open(tmp_path/'input', 'w') as f:
        f.write(input)
    subprocess.run(f'packmol < input', shell=True, cwd=tmp_path)

    # return arbitrary data
    return param

# also a fast and resourse free task
@tag(cache='pickle')
def convert(assemble:dict)->dict:
    from pathlib import Path
    from itertools import cycle

    lmp_run_path = Path('lmp_run')
    lmp_run_path.mkdir(parents=True, exist_ok=True)
    cmd = f"python /home/x_jicli/work/InterMol/intermol/convert.py --amb_in {root}/peo_polymer.prmtop {root}/peo_polymer.inpcrd --lammps"
    subprocess.run(cmd, shell=True, cwd=lmp_run_path)
    # replicate

    lmp = open(f'{lmp_run_path}/peo_polymer_converted.lmp', 'r')
    pdb = open(f'{root}/tmp/peo20.pdb', 'r')
    new_coords = []
    for line in pdb.readlines():
        if line.startswith('ATOM'):
            new_coords.append(line.split()[6:9])
    new_lmp = [f'# peo20 x 20 data\n']
    old_atoms = []
    old_bonds = []
    old_angles = []
    old_dihedrals = []
    lines = lmp
    nunit_sys = 20
    for line in lines:
        if line.endswith('atoms\n'):
            natoms = int(line.split()[0])
            new_lmp.append(f'{natoms*nunit_sys} atoms\n')
        elif line.endswith('bonds\n'):
            nbonds = int(line.split()[0])
            new_lmp.append(f'{nbonds*nunit_sys} bonds\n')
        elif line.endswith('angles\n'):
            nangles = int(line.split()[0])
            new_lmp.append(f'{nangles*nunit_sys} angles\n')
        elif line.endswith('dihedrals\n'):
            ndihedrals = int(line.split()[0])
            new_lmp.append(f'{ndihedrals*nunit_sys} dihedrals\n')
        elif line.endswith('types\n'):
            new_lmp.append(line)
        elif line.endswith('hi\n'):   
            new_lmp.append(line)             

        elif line.startswith('Atoms'):
            
            next(lines)
            new_lmp.append('\nAtoms\n\n')
            for i, line in enumerate(lines):
                if i < natoms:
                    atom_info = line.split()
                    atom_info[1] = '1'
                    old_atoms.append(' '.join(atom_info[:-3]))
                else:
                    break

            for i, (old, coord) in enumerate(zip(cycle(old_atoms), new_coords)):
                old = old.split()
                new_lmp.append(f'{i+1}  {int(i/natoms)+1} {old[2]} {old[3]} {coord[0]} {coord[1]} {coord[2]}\n')
            print('atoms done')
        elif line.startswith('Bonds'):
            next(lines)
            new_lmp.append('\nBonds\n\n')
            for i, line in enumerate(lines):
                if i < nbonds:
                    bond_info = line.split()
                    old_bonds.append(bond_info)
                else:
                    break

            for molid in range(1, nunit_sys+1):
                for i, old in enumerate(old_bonds):
                    new_lmp.append(f'{(molid-1)*natoms + i+1}  {old[1]}  {int(old[2])+(molid-1)*natoms} {int(old[3])+(molid-1)*natoms} \n')

        elif line.startswith('Angles'):
            next(lines)
            new_lmp.append('\nAngles\n\n')
            for i, line in enumerate(lines):
                if i < nangles:
                    angle_info = line.split()
                    old_angles.append(angle_info)
                else:
                    break

            for molid in range(1, nunit_sys+1):
                for i, old in enumerate(old_angles):
                    new_lmp.append(f'{(molid-1)*natoms + i+1}  {old[1]}  {int(old[2])+(molid-1)*natoms} {int(old[3])+(molid-1)*natoms} {int(old[4])+(molid-1)*natoms}\n')

        elif line.startswith('Dihedrals'):
            next(lines)
            new_lmp.append('\nDihedrals\n\n')
            for i, line in enumerate(lines):
                if i < ndihedrals:
                    dihedral_info = line.split()
                    old_dihedrals.append(dihedral_info)
                else:
                    break

            for molid in range(1, nunit_sys+1):
                for i, old in enumerate(old_dihedrals):
                    new_lmp.append(f'{(molid-1)*natoms + i+1}  {old[1]}  {int(old[2])+(molid-1)*natoms} {int(old[3])+(molid-1)*natoms} {int(old[4])+(molid-1)*natoms} {int(old[5])+(molid-1)*natoms}\n')

        elif line.startswith('Velocities'):
            next(lines)
            # new_lmp.append('\nVelocities\n\n')
            for i, line in enumerate(lines):
                if i < natoms:
                    continue
                else:
                    break
    lmp.close()
    pdb.close()

    with open(f'{lmp_run_path}/init.data', 'w') as f:
        f.writelines(new_lmp)

    return assemble