import molexp as me
import prepare
import equilibrium
import examples.slurm.production as production

proj = me.Project('PEO simulation')
paramspace = me.ParamSpace(nchain=[20, 200], simid=range(3))
# will start 2x3=6 simulations
proj.init_exp(paramspace.product())
proj.add_tasks(prepare)
proj.add_tasks(equilibrium)
proj.add_tasks(production)
# input will be {'nchain': 20, simid: 0} underhood
proj.execute(output=['write_script'])
