# author: Roy Kid
# contact: lijichen365@126.com
# date: 2023-11-27
# version: 0.0.1

import inspect
import logging
import shutil
import textwrap
import types
import importlib

from molexp.utils import WorkAt
from .params import Params, Param
from .experiment import Experiment, ExperimentGroup
from pathlib import Path
from hamilton import driver
from hamilton import telemetry
from hamilton.base import SimplePythonGraphAdapter
from hamilton.experimental.h_cache import CachingGraphAdapter
telemetry.disable_telemetry()

__all__ = ["Project"]

class Project:

    def __init__(self, name, root: None | Path | str = None):
        self.name = name
        self.logger = logging.getLogger(self.name)
        self.root = Path(root or Path.cwd())
        self.dir = self.root / self.name
        self.tasks = []
        self.dag_paths = []
        if self.exists():
            self.load()
        else:
            self.init()
        self.meta = open(self.dir / 'meta.txt', 'w+')

    def __del__(self):
        self.meta.close()

    def init(self):
        self.dir.mkdir(parents=True)
        self.logger.info(f"Project {self.name} is created at {self.dir}")
        # TODO: add meta data and config

    def load(self):
        # TODO: check if dir is a valid project
        self.logger.info(f"Project {self.name} is loaded from {self.dir}")

    def exists(self):
        return self.dir.exists()

    def init_exp(self, params: Params):

        expg = ExperimentGroup()
        for param in params:
            exp = Experiment(param, self.dir)
            exp.init()
            expg.append(exp)
            self.meta.write(f"{exp.name}\n")

        return expg
            
    def select(self, params: Params):

        expg = ExperimentGroup()
        for param in params:
            exp = Experiment(param, self.dir)
            if exp.exists():
                expg.append(exp)
        return expg
    
    def select_all(self):
        expg = ExperimentGroup()
        self.meta.seek(0)
        for line in self.meta.readlines():
            exp = Experiment(str(Param.from_str(line)).rstrip('\n'), self.dir)
            exp.load()
            expg.append(exp)
        return expg
        
        
    def add_tasks(self, *modules):
        for module in modules:
            self.tasks.append(module)
            if not Path(self.dir / module.__file__).exists():
                shutil.copy(module.__file__, self.dir)

    def add_task(self, module_name, *funcs):
        with open(self.dir / f"{module_name}.py", "w") as f:
            for func in funcs:
                f.write(textwrap.dedent(inspect.getsource(func)))
        module = importlib.import_module(f"{self.name}.{module_name}")
        self.tasks.append(module)
        return module

    def execute(self, output:list, exp_group: ExperimentGroup | None = None, config={}):

        exp_group = exp_group or self.select_all()
        wa = WorkAt(self.dir)    
        for exp in exp_group:
            dr = (
                driver.Builder()
                .with_modules(*self.tasks)
                .with_config(config)
                .with_adapter(CachingGraphAdapter(str(exp.dir)))
                .build()
            )
            input = dict(exp.param)
            wa.cd_to(exp.dir)
            out = dr.execute(output, inputs=input)
            wa.cd_back()
        return out
