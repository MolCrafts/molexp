import molexp as me

from hamilton.function_modifiers import tag

# @tag(cache='pickle')
def gen_eq_script(work_dir:str, copy_ff:dict)->dict:
    script = me.Script('eq.in')
    script.text = f"""
    # exp: eq
    units real
    atom_style full

    dimension 3
    boundary p p p

    read_data init.data
    include system.ff

    minimize 1e-4 1e-4 1000 1000
    print "minimization done"

    velocity all create 1000.0 ${{rdm}}

    timestep 1.0

    fix sk all shake 0.001 20 0 m 1.008

    variable elapsed equal "elapsed"
    variable tk equal "temp"
    variable pr equal "press"
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

    thermo 10000
    thermo_style custom elapsed temp press vol density etotal
    fix fprint all print 1000 "${{elapsed}} ${{tk}} ${{pr}} ${{vl}} ${{dn}} ${{et}} ${{ep}} ${{ek}}" file eq.thermo screen no

    fix 1 all npt temp 1000.0 1000.0 100.0 iso 1.0 1.0 1000 drag 2.0
    run 100000 every 10000 NULL
    run 1000000
    write_data 1000K.data nocoeff

    fix 1 all npt temp 1000.0 900.0 100.0 iso 1.0 1.0 1000 drag 2.0
    run 100000
    fix 1 all npt temp 900.0 800.0 100.0 iso 1.0 1.0  1000 drag 2.0
    run 100000
    fix 1 all npt temp 800.0 700.0 100.0 iso 1.0 1.0  1000 drag 2.0
    run 100000
    fix 1 all npt temp 700.0 500.0 100.0 iso 1.0 1.0  1000 drag 2.0
    run 200000

    write_data 500K.data nocoeff

    """
    script.substitute(rdm=312)
    script.save(work_dir)
    return copy_ff
