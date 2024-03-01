# author: Roy Kid
# contact: lijichen365@126.com
# date: 2023-12-14
# version: 0.0.1

from pathlib import Path
import random

from molexp.utils import WorkAt
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

    def __init__(self, param: Param, root: Path = Path.cwd()):
        self.param = param
        self.name = str(param)
        self.root = root
        self.dir = self.root / self.name

        self.dir.mkdir(parents=True, exist_ok=True)
