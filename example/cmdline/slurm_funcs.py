import time
from hamilton.function_modifiers import tag

from prototype.cmdline import cmdline_decorator
from prototype.slurm import slurm


@tag(property=["cmdline"], cache="pickle")
@cmdline_decorator
def echo_1(start: str) -> str:
    time.sleep(2)
    return f'echo "1: {start}"'


@tag(property=["cmdline", "slurm"], cache="pickle")
@slurm(
    "./workdir",  # chdir and save submit script to here,
    # moniter = False, # block and monitor task
)
def relax(echo_1: str) -> dict:
    # prepare input files
    time.sleep(2)
    return dict(
        slurm_args={
            "-A": "snic2022-5-658",
            "-n": "128",
            "-J": "relax",
            "-o": "out",
            "-e": "err",
            "-t": "07-00:00:00",
        },
        cmd=["mpprun lmp -in in.relax"],
    )


@tag(property=["cmdline", "slurm"], cache="pickle")
@slurm(
    "./workdir",  # chdir and save submit script to here,
    # candidate API:
    # moniter = True
)
def test(relax: dict) -> [dict, dict, dict]:
    time.sleep(2)
    slurm_dict = dict(
        slurm_args={
            "-A": "snic2022-5-658",
            "-n": "128",
            "-J": "test",
            "-o": "out",
            "-e": "err",
            "-t": "07-00:00:00",
        },
        cmd=["mpprun lmp -in in.relax"],
    )
    process_result = yield slurm_dict
    # do postprocess
    # or get a result from a file
    return {"result": 42, "process_result": process_result}


@tag(property=["cmdline"], cache="pickle")
@cmdline_decorator
def echo_3(test: dict, relax: dict) -> str:
    return f'echo "3: {str(test) + ":::" + str(relax)}"'
