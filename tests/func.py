import molexp as me
from pathlib import Path
from hamilton.htypes import Parallelizable, Collect
from molexp.submitor import submit

def pre_process(params: me.ParamSpace)->Parallelizable[me.Experiment]:

    print("pre_process: ", Path.cwd())

    for param in params.product():

        yield me.Experiment(param, Path.cwd())

@submit()
def main(pre_process: me.Experiment)->me.Experiment:

    sleep_time = pre_process.param['sleep_time']
    print(str(pre_process.dir))
    # do something
    print(f"before submit")
    yield dict(
        queue="slurm",
        job_name=pre_process.name,
        working_directory=str(pre_process.dir),
        cores=16,
        memory_max="8G",
        run_time_max= 1e5,
        dependency_list=None,
        monitor=False,
        command=f"sleep {sleep_time}"
    )
    print(f"after submit")

    return pre_process

def post_process(main: Collect[me.Experiment])->me.ExperimentGroup:

    return me.ExperimentGroup(main)
