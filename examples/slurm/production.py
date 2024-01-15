import logging
from pathlib import Path
import os
import molexp as me
import random

logger = logging.getLogger(__name__)

# function for property calculation
@tag(cache='pickle')
def property_calculate(cd:Path)-> Path:
    script = me.Script('tg', 'in')
    script.append("""# init & eq & tg
units real
atom_style full
read_data ../400K.data
include ../system.ff
kspace_style pppm 1e-5
neigh_modify one 10000

thermo 1000
thermo_style custom step temp press vol density etotal
variable step equal "elapsed"
variable tk equal "temp"
variable vl equal "vol"
variable dn equal "density"
variable et equal "etotal"
variable ep equal "pe"
variable ek equal "ke"
variable evdwl equal "evdwl"
variable ecoul equal "ecoul"
variable epair equal "epair"
variable eb equal "ebond"
variable eang equal "eangle"
variable edih equal "edihed"

fix print all print 10000 "${step} ${tk} ${vl} ${dn} ${et} ${ep} ${ek}" file eq.thermo screen no 
velocity all create 500.0 ${rdm}

thermo 10000

reset_timestep 0

variable tksteps equal 1000000
fix print all print 1000 "${step} ${tk} ${vl} ${dn} ${et} ${ep} ${ek} ${evdwl} ${ecoul} ${epair} ${eb} ${eang} ${edih}" file tg.thermo screen no 
fix             printcooling all ave/time  100 5000 ${tksteps} v_tk v_vl v_dn v_et v_ep v_ek v_evdwl v_ecoul v_epair v_eb v_eang v_edih file tg_step.thermo 

variable        tkint equal 500 #Starting temperature
variable        tkend equal 200 #Ending temperature
variable        ntksteps equal (${tkint}-${tkend})/10
        
variable        i loop ${ntksteps}
label           tgcooling
variable        tkstep equal ${tkint}-${i}*10
print           "loop ${i}: Temperature at annealing step = ${tkstep} K"
fix             1 all npt temp ${tkstep} ${tkstep} 100 iso 0 0 1000 drag 2
fix             2 all momentum 1 linear 1 1 1

run             ${tksteps} # 1ns
unfix           1
unfix           2

next            i
jump            tg.in tgcooling
write_data      cooling.data
        
variable  j loop ${ntksteps}
label tgheating
variable tkstep equal ${tkend}+${j}*10
print           "Temperature at annealing step = ${tkstep} K"
fix             1 all npt temp ${tkstep} ${tkstep} 100 iso 0 0 1000 drag 2
fix             2 all momentum 1 linear 1 1 1
run             ${tksteps} # 1ns
unfix           1
unfix           2
next            j
jump            tg.in tgheating
write_data      heating.data   
        """)
    script.substitute(rdm=random.randint(1, 10000))
    script.save(cd)
    return cd

def submit_tg(add_tg_script:Path, spec:str, z:int) -> Path:
    cwd = os.getcwd()
    os.chdir(add_tg_script)
    submitor = me.SlurmSubmitor({
        # '-A': 'snic2022-5-658',  # small
        '-A': 'naiss2023-1-37',  # large
        '-n': 64,
        '-J': f'tg_{spec}_z{z}',
        '-o': 'tg.out',
        '-t': '07-00:00:00',
    })
    submitor.append('mpprun lmp -in tg.in')
    submitor.submit(add_tg_script)
    print(f'submit {add_tg_script}')
    os.chdir(cwd)
    return add_tg_script