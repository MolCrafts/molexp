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
import json

from molexp.utils import WorkAt
from .params import Params, Param
from .experiment import Experiment, ExperimentGroup
from pathlib import Path
from hamilton import driver
from hamilton import telemetry
from hamilton.base import SimplePythonGraphAdapter
from hamilton.experimental.h_cache import CachingGraphAdapter
from hamilton.execution import executors
telemetry.disable_telemetry()

__all__ = ["Project"]

class Project:

    def __init__(self, name, root: None | Path | str = None):
        self.name = name
        self.logger = logging.getLogger(self.name)
        self.root = Path(root or Path.cwd())
        self.dir = self.root / self.name
        self.cache_dir = self.dir / "cache"
        self.tasks = {
            "pre": [],
            "main": [],
            "post": []
        }
        self.expg = ExperimentGroup()
        if self.exists():
            self.load()
        else:
            self.init()

    def init(self):
        self.dir.mkdir(parents=True)
        self.logger.info(f"Project {self.name} is created at {self.dir}")
        self.index = {
            "version": "0.0.1",
            "name": self.name,
            "experiments": [],
            "tasks": {
                "pre": [],
                "main": [],
                "post": []
            },
            "cache_dir": f"{self.dir} / cache"
        }
        # TODO: add index data and config

    def __del__(self):
        with open(self.dir / "index.json", "w") as f:
            json.dump(self.index, f)

    def load(self):
        with open(self.dir / "index.json", "r") as f:
            self.index = json.load(f)
        for exp_name in self.index['experiments']:
            exp = Experiment(Param.from_str(exp_name), self.dir)
            exp.load()
            self.expg.append(exp)
        self.logger.info(f"Project {self.name} is loaded from {self.dir}")

    def exists(self):
        return self.dir.exists()

    def init_exp(self, params: Params):

        for param in params:
            exp = Experiment(param, self.dir)
            exp.init()
            self.expg.append(exp)
            self.index['experiments'].append(exp.name)
        return self.expg
            
    def select(self, params: Params) -> ExperimentGroup:

        expg = ExperimentGroup()
        for param in params:
            exp = Experiment(param, self.dir)
            if exp.exists():
                expg.append(exp)
        return expg
    
    def add_pre_task(self, module_name):

        self.tasks["pre"].append(module_name)
        self.index["tasks"]["pre"] = [module.__name__ for module in self.tasks["pre"]]
        
        for module in self.tasks["p"]:
            shutil.copy(module.__file__, self.dir)

    def add_task(self, module_name):
        
        self.tasks["main"].append(module_name)
        self.index["tasks"]["main"] = [module.__name__ for module in self.tasks]
        
        for module in self.tasks["main"]:
            shutil.copy(module.__file__, self.dir)

    def add_post_task(self, module_name):

        self.tasks["post"].append(module_name)
        self.index["tasks"]["post"] = [module.__name__ for module in self.tasks["post"]]
        
        for module in self.tasks["post"]:
            shutil.copy(module.__file__, self.dir)


    def execute(self, output:list, config={}):

        proj_dir = WorkAt(self.dir)
        proj_dir.cd_to(self.dir)
        dr = (
            driver.Builder()
            .with_modules(*self.tasks["pre"])
            .with_config(config)
            .with_adapters(CachingGraphAdapter(self.cache_dir))
            # .with_local_executor()
            # .with_remote_executor()
            .build()
        )
        dr.execute(output, inputs=input)

        exp_group = exp_group or self.select_all()
   
        for exp in exp_group:
            dr = (
                driver.Builder()
                .with_modules(*self.tasks["main"])
                .with_config(config)
                .with_adapter(CachingGraphAdapter(str(exp.cache_dir)))
                .build()
            )
            info = {'param': exp.param, 'exp_dir': exp.dir}
            proj_dir.cd_to(exp.dir)
            dr.execute(output, inputs={'info': info})
            proj_dir.cd_back()

        dr = (
            driver.Builder()
            .with_modules(*self.tasks["post"])
            .with_config(config)
            .with_adapters(CachingGraphAdapter(self.cache_dir))
            # .with_local_executor()
            # .with_remote_executor()
            .build()            
        )
        dr.execute(output, inputs=input)