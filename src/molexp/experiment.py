# author: Roy Kid
# contact: lijichen365@126.com
# date: 2023-12-14
# version: 0.0.1

from pathlib import Path
import random
from .params import Param
from typing import Callable

class ExperimentGroup(list):
    pass

    def map(self, func: Callable):
        result = {}
        for exp in self.expg:
            result[exp.name] = func(exp)
        return result

class Experiment:

    def __init__(self, param: Param, root: None | Path | str):
        self.param = param
        self.name = str(param)
        self.root = Path(root)
        self.dir = self.root / self.name
        self.id = hex(random.randint(0, 255))

    def init(self):
        self.dir.mkdir(parents=True, exist_ok=True)

    def exists(self):
        return self.dir.exists()
    
    def load(self):
        pass
