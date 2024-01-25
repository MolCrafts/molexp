import time
from pathlib import Path
from subprocess import CompletedProcess

from hamilton.function_modifiers import tag

from prototype.cmdline import cmdline_decorator
from prototype.slurm import slurm


@tag(cmdline="yes", cache="pickle")
@slurm()
def echo_1(start: str) -> str:
    time.sleep(2)
    return f'echo "1: {start}"'


@tag(cmdline="yes", slurm="yes", cache="pickle")
@slurm(
    name = "relax",
    work_dir = "."  # chdir and save submit script to here,
    {
        "-A": "snic2022-5-658",
        "-n": "128",
        "-J": "relax",
        "-o": "out",
        "-e": "err",
        "-t": "07-00:00:00",
    },
    cmd = ["mpprun lmp -in in.relax"],
    # candidate API:
    # moniter = False, # block and monitor task
)
def echo_2(echo_1: str) -> str:
    # prepare input files
    time.sleep(2)
    return "done"  # return result

@tag(cmdline="yes", cache="pickle")
@slurm(
    name = "test",
    work_dir = "."  # chdir and save submit script to here,
    {
        "-A": "snic2022-5-658",
        "-n": "128",
        "-J": "test",
        "-o": "out",
        "-e": "err",
        "-t": "07-00:00:00",
    },
    cmd = ["mpprun lmp -in in.relax"],
    # candidate API:
    # moniter = True
)
def echo_2b(echo_1: str) -> [str, CompletedProcess, str]:

    time.sleep(2)
    text_log_path = yield lambda: Path("text.log")
    # do postprocess
    # or get a result from a file
    return 42


@tag(cmdline="yes", cache="pickle")
@cmdline_decorator
def echo_3(echo_2: str, echo_2b: str) -> str:
    return f'echo "3: {echo_2 + ":::" + echo_2b}"'
