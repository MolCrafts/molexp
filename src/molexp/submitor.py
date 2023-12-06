# author: Roy Kid
# contact: lijichen365@126.com
# date: 2023-12-01
# version: 0.0.1

from .script import Script
from pathlib import Path
import subprocess

__all__ = ['SlurmSubmitor']

class SlurmSubmitor(Script):

    def __init__(self, config:dict, name:str='submit', ext:str='sh'):
        super().__init__(name, ext)
        self._config = config

    def submit(self, path: Path | str = Path.cwd()):
        # write down
        config_lines = []
        config_lines.append('#!/bin/bash')
        for k, v in self._config.items():
            config_lines.append(f'#SBATCH {k} {v}')
        config_lines.append('\n')

        self._content_list = config_lines + self._content_list

        path = Path(path)
        self.save(path)

        subprocess.call(f'sbatch {self.full_name}', shell=True, cwd=path)
        print(f'Submit {self.full_name} to {path} successfully!')

def submit(func, config:dict, name:str='submit', ext:str='sh', workdir:Path=Path.cwd()):
    slurm_submit = SlurmSubmitor(config, name, ext)
    func()
    