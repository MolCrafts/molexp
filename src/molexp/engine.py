import shutil
import subprocess
from pathlib import Path

from .script import Script

class Engine:
    ...

class LAMMPSEngine(Engine):

    def __init__(self, which: str):
        self.which = Path(shutil.which(which))
        assert self.which.exists(), f"{self.which} does not exist"

    def add_script(self, script: Script):
        self.script = script

    def run(self, cmd: str, cwd: Path | str = Path.cwd()):
        self.script.prettify()
        self.script.save(cwd)
        subprocess.run(cmd, shell=True, cwd=cwd, check=True)