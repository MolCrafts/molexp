# author: Roy Kid
# contact: lijichen365@126.com
# date: 2023-09-15
# version: 0.0.1

from typing import Any
from metaflow import step

class Task:
    def __init__(self, name):
        self.name = name

from subprocess import run

class ShellTask(Task):
    
    def __init__(self, name):
        super().__init__(name)
        
    @step
    def __call__(self, commands: str, *args, **kwargs) -> Any:
        run(commands, shell=True, *args, **kwargs)