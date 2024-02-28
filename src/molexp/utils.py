from contextlib import contextmanager
from functools import partial
import os
from pathlib import Path
from hamilton.function_modifiers import tag

class WorkAt:

    def __init__(self, root: str | Path):
        self.root = Path(root)

    def cd_to(self, path: str | Path):
        os.chdir(path)

    def cd_back(self):
        os.chdir(self.root)

    def __enter__(self):
        self.cd_to()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cd_back()

@contextmanager
def work_at(path):
    workat = WorkAt(path)
    workat.cd_to()
    yield
    workat.cd_back()

def cmdrun(cmd: str, *arg, **kwargs):
    import subprocess
    subprocess.run(cmd, shell=True, check=True, *arg, **kwargs) 

cache = partial(tag, cache='pickle')