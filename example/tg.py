import logging
from pathlib import Path
import os
import molexp as me
import random
import subprocess

logger = logging.getLogger(__name__)

def gen_tg_script(work_dir:str, gen_eq_script:dict)-> dict:
    script = me.Script('tg.in')
    script.append("""# init tg
    units real
    atom_style full
    read_data 500K.data
    include system.ff
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

    reset_timestep 0
    velocity all create 500.0 ${rdm}

    variable tksteps equal 1000000
    fix print all print 1000 "${step} ${tk} ${vl} ${dn} ${et} ${ep} ${ek} ${evdwl} ${ecoul} ${epair} ${eb} ${eang} ${edih}" file tg.thermo screen no 
    fix             printcooling all ave/time  100 2000 ${tksteps} v_tk v_vl v_dn v_et v_ep v_ek v_evdwl v_ecoul v_epair v_eb v_eang v_edih file tg_step.thermo 
                    
    restart  1000000 tg.restart

    variable        tkint equal 500 #Starting temperature
    variable        tkend equal 200 #Ending temperature
    variable        ntksteps equal (${tkint}-${tkend})/20
            
    variable        i loop ${ntksteps}
    label           tgcooling
    variable        tkstep equal ${tkint}-${i}*10
    print           "loop ${i}: Temperature at annealing step = ${tkstep} K"
    fix             1 all npt temp ${tkstep} ${tkstep} 100 iso 1.0 1.0 1000 drag 2
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
    fix             1 all npt temp ${tkstep} ${tkstep} 100 iso 1.0 1.0 1000 drag 2
    fix             2 all momentum 1 linear 1 1 1
    run             ${tksteps} # 1ns
    unfix           1
    unfix           2
    next            j
    jump            tg.in tgheating
    write_data      heating.data   
        """)
    script.substitute(rdm=random.randint(1, 10000))
    script.save(work_dir)

    return gen_eq_script

@me.submit()
def submit(work_dir:str, repeat_unit:list[str], repeat:int, gen_tg_script:dict)->dict:
    job_name = f"{''.join(repeat_unit)}x{repeat}"
    tmp = dict(
        queue="slurm",
        job_name=job_name,
        working_directory=work_dir,
        ncores=128,
        # memory_max="8G",
        run_time_max= 1e5,
        dependency_list=None,
        monitor=False,
        command=f"mpprun ~/.local/bin/lmp -in eq.in\nmpprun ~/.local/bin/lmp -in tg.in",
    )
    tmp['account'] = 'naiss2023-1-37'
    # return submit config
    yield tmp
    return {'work_dir': work_dir}