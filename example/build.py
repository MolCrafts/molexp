import numpy as np
import subprocess
import nglview as ngl
from ase.io import read, write
from pathlib import Path
from hamilton.function_modifiers import tag
from itertools import cycle

units_path = Path("./units")
units_path.mkdir(parents=True, exist_ok=True)
tmp_path = Path("./tmp")
tmp_path.mkdir(parents=True, exist_ok=True)
head_name = "H"
n_name = "N"
m_name = "M"
tail_name = "T"
pegm_name = "P"


@tag(cache="pickle")
def parameterize() -> dict:
    cmd = f"antechamber -i ../{units_path/f'H.pdb'} -fi pdb -o {f'H.ac'} -fo ac -at gaff -an y -c bcc -nc 0 -rn H  && mv sqm.pdb H.pdb"
    subprocess.run(cmd, shell=True, cwd=tmp_path, check=True)
    cmd = f"antechamber -i ../{units_path/f'N.pdb'} -fi pdb -o {f'N.ac'} -fo ac -at gaff -an y -c bcc -nc 1 -rn N  && mv sqm.pdb N.pdb"
    subprocess.run(cmd, shell=True, cwd=tmp_path, check=True)
    cmd = f"antechamber -i ../{units_path/f'M.pdb'} -fi pdb -o {f'M.ac'} -fo ac -at gaff -an y -c bcc -nc -1 -rn M  && mv sqm.pdb M.pdb"
    subprocess.run(cmd, shell=True, cwd=tmp_path, check=True)
    cmd = f"antechamber -i ../{units_path/f'T.pdb'} -fi pdb -o {f'T.ac'} -fo ac -at gaff -an y -c bcc -nc 0 -rn T  && mv sqm.pdb T.pdb"
    subprocess.run(cmd, shell=True, cwd=tmp_path, check=True)
    return {
        'acs': [f'H.ac', f'N.ac', f'M.ac', f'T.ac'],
    }


@tag(cache="pickle")
def define_connect(parameterize: dict) -> dict:

    H_tail = 14
    H_omit = [15, 16, 17]

    N_head = 6
    N_tail = 14
    N_omit = [9, 31]

    M_head = 5
    M_tail = 14
    M_omit = [0, 9]

    T_head = 0
    T_omit = [8]

    # P_head = 5
    # P_tail = 0
    # P_omit = [6, 7, 8, 2, 3, 4]

    H = open(f"{tmp_path}/H", "w")
    h_ac = open(f"{tmp_path}/{head_name}.ac", "r")
    h = h_ac.readlines()[2:]
    h_ac.close()

    N = open(f"{tmp_path}/N", "w")
    n_ac = open(f"{tmp_path}/{n_name}.ac", "r")
    n = n_ac.readlines()[2:]
    n_ac.close()

    M = open(f"{tmp_path}/M", "w")
    m_ac = open(f"{tmp_path}/{m_name}.ac", "r")
    m = m_ac.readlines()[2:]
    m_ac.close()

    T = open(f"{tmp_path}/T", "w")
    t_ac = open(f"{tmp_path}/{tail_name}.ac", "r")
    t = t_ac.readlines()[2:]
    t_ac.close()

    # pegm = open(f'{tmp_path}/{pegm_name}.chain', 'w')
    # p_ac = open(f'{tmp_path}/{pegm_name}.ac', 'r')
    # p = p_ac.readlines()[2:]
    # p_ac.close()

    H.write("TAIL_NAME " + str(h[H_tail].split()[2]) + "\n")
    for i in range(len(H_omit)):
        H.write("OMIT_NAME " + str(h[int(H_omit[i])].split()[2]) + "\n")
    H.write("CHARGE 0")
    H.close()

    N.write("HEAD_NAME " + str(n[N_head].split()[2]) + "\n")
    N.write("TAIL_NAME " + str(n[N_tail].split()[2]) + "\n")
    for i in range(len(N_omit)):
        N.write("OMIT_NAME " + str(n[int(N_omit[i])].split()[2]) + "\n")
    N.write("CHARGE 1")
    N.close()

    M.write("HEAD_NAME " + str(m[M_head].split()[2]) + "\n")
    M.write("TAIL_NAME " + str(m[M_tail].split()[2]) + "\n")
    for i in range(len(M_omit)):
        M.write("OMIT_NAME " + str(m[int(M_omit[i])].split()[2]) + "\n")
    M.write("CHARGE -1")
    M.close()

    T.write("HEAD_NAME " + str(t[T_head].split()[2]) + "\n")
    for i in range(len(T_omit)):
        T.write("OMIT_NAME " + str(t[int(T_omit[i])].split()[2]) + "\n")
    T.write("CHARGE 0")
    T.close()
    return parameterize


# @tag(cache="pickle")
def prepare(define_connect: dict) -> dict:
    print('prep')
    cmd = f"prepgen -i H.ac -o H.prepi -f prepi -m H -rn H -rf H.res"
    subprocess.run(cmd, shell=True, cwd=tmp_path, check=True)
    cmd = f"prepgen -i N.ac -o N.prepi -f prepi -m N -rn N -rf N.res"
    subprocess.run(cmd, shell=True, cwd=tmp_path, check=True)
    cmd = f"prepgen -i M.ac -o M.prepi -f prepi -m M -rn M -rf M.res"
    subprocess.run(cmd, shell=True, cwd=tmp_path, check=True)
    # cmd = f"prepgen -i M.ac -o NMN.prepi -f prepi -m nmn -rn NMN -rf NMN.res"
    # subprocess.run(cmd, shell=True, cwd=tmp_path)
    # cmd = f"prepgen -i M.ac -o NMT.prepi -f prepi -m nmt -rn NMT -rf NMT.res"
    # subprocess.run(cmd, shell=True, cwd=tmp_path)
    cmd = f"prepgen -i T.ac -o T.prepi -f prepi -m T -rn T -rf T.res"
    subprocess.run(cmd, shell=True, cwd=tmp_path, check=True)
    return define_connect

# @tag(cache="pickle")
def parmchk(prepare: dict) -> dict:

    parm = lambda name: subprocess.run(
        f"parmchk2 -i {name}.prepi -f prepi -o {name}.frcmod", shell=True, cwd=tmp_path
    )

    parm("H")
    parm("N")
    parm("M")
    parm("T")
    return prepare


# @tag(cache="pickle")
def connect(work_dir:str, repeat_unit: list[str], repeat: int, parmchk: dict) -> dict:

    seq = "H " + " ".join([" ".join(repeat_unit)]*repeat) + " T"
    print(f"{seq=}")
    input_file = f"tleap.in"
    output_file = f"tleap.out"
    filename = f"{''.join(repeat_unit)}"
    amb_prmtop = f"{filename}.prmtop"
    amb_inpcrd = f"{filename}.inpcrd"
    tleap = open(f"tmp/{input_file}", "w")
    tleap.write(
        f"""
    source leaprc.gaff
    loadamberprep H.prepi
    loadamberprep N.prepi
    loadamberprep M.prepi
    loadamberprep T.prepi
    loadamberparams H.frcmod
    loadamberparams N.frcmod
    loadamberparams M.frcmod
    loadamberparams T.frcmod

    system = sequence {{ {seq} }}

    """
    )

    tleap.write(
        f"""

    savepdb system {filename}.pdb
    saveamberparm system {amb_prmtop} {amb_inpcrd}
    quit
    """
    )
    tleap.close()

    cmd = f"tleap -s -f {input_file} > {output_file}"
    subprocess.run(cmd, shell=True, cwd=tmp_path, check=True)

    cmd = f"python /home/x_jicli/work/InterMol/intermol/convert.py --amb_in ../tmp/{amb_prmtop} ../tmp/{amb_inpcrd} --lammps"
    subprocess.run(cmd, shell=True, cwd=work_dir, check=True)
    parmchk["filename"] = f"{filename}"
    return parmchk


# @tag(cache="pickle")
def pack(n_chains: int, density: float, connect: dict) -> dict:

    filename = connect["filename"]

    with open(f"tmp/{filename}.pdb") as f:
        pdb = f.readlines()
        natoms = len(pdb) - 3

    box_length = (natoms*n_chains / density) ** (1 / 3)
    connect["box_length"] = box_length
    print(f"{natoms=}")
    print(f"{box_length=}")
    script = f"""tolerance 2.0
    output {filename}_n{n_chains}.pdb
    filetype pdb

    structure {filename}.pdb
    number {n_chains}
    inside cube 0. 0. 0. {box_length}
    end structure
    """
    with open(f"{tmp_path}/input", "w") as f:
        f.write(script)
    subprocess.run(f"packmol < input", shell=True, cwd=tmp_path)
    return connect


# @tag(cache="pickle")
def to_lammps(work_dir:str, n_chains: int, pack: dict) -> dict:

    filename = pack["filename"]
    box_length = pack["box_length"]

    lmp = open(f"{work_dir}/{filename}_converted.lmp", "r")
    pdb = open(f"tmp/{filename}_n{n_chains}.pdb", "r")
    new_coords = []
    for line in pdb.readlines():
        if line.startswith("ATOM"):
            new_coords.append(line.split()[6:9])
    new_lmp = [f"# {filename}_n{n_chains} data\n"]
    old_atoms = []
    old_bonds = []
    old_angles = []
    old_dihedrals = []
    lines = lmp
    for line in lines:
        if line.endswith("atoms\n"):
            natoms = int(line.split()[0])
            new_lmp.append(f"{natoms*n_chains} atoms\n")
        elif line.endswith("bonds\n"):
            nbonds = int(line.split()[0])
            new_lmp.append(f"{nbonds*n_chains} bonds\n")
        elif line.endswith("angles\n"):
            nangles = int(line.split()[0])
            new_lmp.append(f"{nangles*n_chains} angles\n")
        elif line.endswith("dihedrals\n"):
            ndihedrals = int(line.split()[0])
            new_lmp.append(f"{ndihedrals*n_chains} dihedrals\n")
        elif line.endswith("types\n"):
            new_lmp.append(line)
        elif line.endswith("xlo xhi\n"):
            new_lmp.append(f"0.0 {box_length} xlo xhi\n")
        elif line.endswith("ylo yhi\n"):
            new_lmp.append(f"0.0 {box_length} ylo yhi\n")
        elif line.endswith("zlo zhi\n"):
            new_lmp.append(f"0.0 {box_length} zlo zhi\n")

        elif line.startswith("Atoms"):

            next(lines)
            new_lmp.append("\nAtoms\n\n")
            for i, line in enumerate(lines):
                if i < natoms:
                    atom_info = line.split()
                    atom_info[1] = "1"
                    old_atoms.append(" ".join(atom_info[:-3]))
                else:
                    break

            assert len(old_atoms) == natoms

            new_atoms = []
            for i, (old, coord) in enumerate(zip(cycle(old_atoms), new_coords)):
                old = old.split()
                new_atoms.append(
                    f"{i+1}  {int(i/natoms)+1} {old[2]} {old[3]} {coord[0]} {coord[1]} {coord[2]}\n"
                )
            assert len(new_atoms) == natoms * n_chains
            new_lmp.extend(new_atoms)

        elif line.startswith("Bonds"):
            next(lines)
            new_lmp.append("\nBonds\n\n")
            for i, line in enumerate(lines):
                if i < nbonds:
                    bond_info = line.split()
                    old_bonds.append(bond_info)
                else:
                    break
            assert len(old_bonds) == nbonds

            new_bonds = []
            for molid in range(1, n_chains + 1):
                for i, old in enumerate(old_bonds, 1):
                    bond_offset = (molid - 1) * nbonds
                    atom_offset = (molid - 1) * natoms
                    new_bonds.append(
                        f"{bond_offset + i}  {old[1]}  {int(old[2])+atom_offset} {int(old[3])+atom_offset} \n"
                    )
            assert len(new_bonds) == nbonds * n_chains
            new_lmp.extend(new_bonds)

        elif line.startswith("Angles"):
            next(lines)
            new_lmp.append("\nAngles\n\n")
            for i, line in enumerate(lines):
                if i < nangles:
                    angle_info = line.split()
                    old_angles.append(angle_info)
                else:
                    break

            assert len(old_angles) == nangles

            new_angles = []
            for molid in range(1, n_chains + 1):
                for i, old in enumerate(old_angles, 1):
                    angle_offset = (molid - 1) * nangles
                    atom_offset = (molid - 1) * natoms
                    new_angles.append(
                        f"{angle_offset + i}  {old[1]}  {int(old[2])+atom_offset} {int(old[3])+atom_offset} {int(old[4])+atom_offset}\n"
                    )
            assert len(new_angles) == nangles * n_chains
            new_lmp.extend(new_angles)

        elif line.startswith("Dihedrals"):
            next(lines)
            new_lmp.append("\nDihedrals\n\n")
            for i, line in enumerate(lines):
                if i < ndihedrals:
                    dihedral_info = line.split()
                    old_dihedrals.append(dihedral_info)
                else:
                    break

            assert len(old_dihedrals) == ndihedrals
            new_dihedrals = []
            for molid in range(1, n_chains + 1):
                for i, old in enumerate(old_dihedrals, 1):
                    dihedral_offset = (molid - 1) * ndihedrals
                    atom_offset = (molid - 1) * natoms
                    new_dihedrals.append(
                        f"{dihedral_offset + i}  {old[1]}  {int(old[2])+atom_offset} {int(old[3])+atom_offset} {int(old[4])+atom_offset} {int(old[5])+atom_offset}\n"
                    )
            assert len(new_dihedrals) == ndihedrals * n_chains
            new_lmp.extend(new_dihedrals)

        elif line.startswith("Velocities"):
            next(lines)
            # new_lmp.append('\nVelocities\n\n')
            for i, line in enumerate(lines):
                if i < natoms:
                    continue
                else:
                    break

    with open(f"{work_dir}/init.data", "w") as f:
        print(f"asdfasdfas: {work_dir}/init.data")
        f.writelines(new_lmp)
    print('ghadsa')
    return pack

# @tag(cache="pickle")
def copy_ff(work_dir:str, to_lammps: dict) -> dict:

    filename = to_lammps["filename"]

    from_lmp = []
    from_input = []

    natomtypes = 0
    nbondtypes = 0
    nangletypes = 0
    ndihedraltypes = 0

    with open(f'{work_dir}/{filename}_converted.lmp', 'r') as f:
        for line in f:

            if 'atom types' in line:
                natomtypes = int(line.split()[0])
                continue
            
            elif 'bond types' in line:
                nbondtypes = int(line.split()[0])
                continue

            elif 'angle types' in line:
                nangletypes = int(line.split()[0])
                continue

            elif 'dihedral types' in line:
                ndihedraltypes = int(line.split()[0])
                continue

            elif line.startswith('Masses'):
                next(f)
                for _ in range(natomtypes):
                    line = next(f)
                    from_lmp.append(f"mass {line}")
                continue

            elif line.startswith('Bond Coeffs'):
                next(f)
                for _ in range(nbondtypes):
                    line = next(f)
                    from_lmp.append(f"bond_coeff {line}")
                continue

            elif line.startswith('Angle Coeffs'):
                next(f)
                for _ in range(nangletypes):
                    line = next(f)
                    from_lmp.append(f"angle_coeff {line}")
                continue

            elif line.startswith('Dihedral Coeffs'):
                next(f)
                for _ in range(ndihedraltypes):
                    line = next(f)
                    from_lmp.append(f"dihedral_coeff {line}")
                continue

    assert natomtypes
    assert nbondtypes
    assert nangletypes
    assert ndihedraltypes

    with open (f'{work_dir}/{filename}_converted.input', 'r') as f:
        for line in f:
            if line.startswith('bond_style'):
                from_input.append(line)
            elif line.startswith('angle_style'):
                from_input.append(line)
            elif line.startswith('dihedral_style'):
                from_input.append(line)
            elif line.startswith('improper_style'):
                from_input.append(line)
            elif line.startswith('pair_style'):
                from_input.append(line)
            elif line.startswith('kspace_style'):
                from_input.append(line)
            elif line.startswith('special_bonds'):
                from_input.append(line)
            elif line.startswith('pair_modify'):
                from_input.append(line)
            elif line.startswith('pair_coeff'):
                from_lmp.append(line)

    system_ff = from_input + from_lmp
    with open(f'{work_dir}/system.ff', 'w') as f:
        f.writelines(system_ff)

    return to_lammps
