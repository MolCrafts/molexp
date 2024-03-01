# author: Roy Kid
# contact: lijichen365@126.com
# date: 2023-11-27
# version: 0.0.1

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
        self.root = Path(root or Path.cwd())
        self.dir = self.root / self.name
        self.cache_dir = self.dir / "cache"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        WorkAt(self.dir)
        print('proj: ', Path.cwd())

    def add_tasks(self, *modules):
        self.models = modules
            
    def execute(self, inputs:dict, final_vars: list):
        
        dr = (
            driver.Builder()
            .with_modules(*self.models)
            .with_adapters(CachingGraphAdapter(self.cache_dir))
            .enable_dynamic_execution(allow_experimental_mode=True)
            # .with_config()
            .with_local_executor(executors.SynchronousLocalTaskExecutor())
            .with_remote_executor(executors.MultiProcessingExecutor(max_tasks=4))
            .build()
        )
        inputs.update({
            "proj_dir": self.dir
        })
        dr.execute(
            final_vars=final_vars,
            inputs=inputs
        )