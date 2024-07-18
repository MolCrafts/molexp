import os
from pathlib import Path
from types import ModuleType
from typing import Any

from hamilton import driver
from hamilton.execution import executors
from hamilton.execution.executors import DefaultExecutionManager
from hamilton.lifecycle import GraphAdapter
try:
    from hamilton_sdk import adapters
    SDK_INSTALL = True
except ImportError:
    SDK_INSTALL = False
from hamilton.lifecycle.default import CacheAdapter

import molexp as me
from molexp.experiment import Experiment


class Project:
    def __init__(
        self,
        name: str,
        work_dir: str | Path = Path.cwd(),
        tags: dict = {},
        description: str = "",
        proj_id: int | None = None,
    ):
        self.name = name
        self._work_dir = Path(work_dir).absolute() / name
        self._work_dir.mkdir(exist_ok=True)
        self.description = description

        self.execution_manager = DefaultExecutionManager(
            executors.SynchronousLocalTaskExecutor(),
            executors.MultiThreadingExecutor(8),
        )
        self.experiments = me.experiment.Experiments()
        self.tracker = None
        if SDK_NOT_INSTALL and proj_id:
            assert me.Config.USER_NAME != "undefined", ValueError(
                "Please set your username in molexp.Config.USER_NAME"
            )
            self.tracker = adapters.HamiltonTracker(
                project_id=proj_id,
                username=me.Config.USER_NAME,
                dag_name=name,
                tags=tags,
            )

    @property
    def work_dir(self) -> Path:
        return self._work_dir

    def def_exp(self, name: str, param: me.Param):
        exp = Experiment(
            name=name,
            param=param,
        )
        self.add_exp(exp)
        return exp

    def add_exp(self, exp: Experiment):
        self.experiments.add(exp)

    def get_exp(self, name: str) -> Experiment:
        return self.experiments.get_by_name(name)

    def ls(self):
        return self.experiments

    def _get_driver(self, modules: list[ModuleType], adapters: list[GraphAdapter]):

        dr = driver.Builder().with_modules(*modules).with_adapters(*adapters).build()
        return dr

    def start_task(self, path: str, final_vars: list[str] = []):

        exp_name, task_name = path.split("/")
        exp = self.experiments.get_by_name(exp_name)
        task = exp.tasks.get_by_name(task_name)
        exp_tracker = exp.get_tracker(self._work_dir)  # inside proj path
        task_tracker = task.get_tracker(exp_tracker.work_dir)  # inside exp path
        modules = []
        modules.extend(exp.modules)
        modules.extend(task.modules)
        adapters = [exp_tracker, task_tracker]
        if self.tracker:
            adapters.append(self.tracker)

        cache_dir = task_tracker.work_dir / ".cache" / "cache"
        cache_adapter = CacheAdapter(cache_path=str(cache_dir))
        adapters.append(cache_adapter)

        dr = self._get_driver(
            modules=modules,
            adapters=adapters,
        )

        if not final_vars:
            final_vars = dr.list_available_variables()

        params = exp.get_param()
        params.update(task.get_param())

        return dr.execute(final_vars=final_vars, inputs=params)

