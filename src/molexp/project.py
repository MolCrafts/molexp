import warnings
from pathlib import Path
from types import ModuleType

from hamilton import driver
from hamilton.execution import executors
from hamilton.execution.executors import DefaultExecutionManager
from hamilton.lifecycle import GraphAdapter

try:
    from hamilton_sdk import adapters

    SDK_INSTALL = True
except ImportError:
    SDK_INSTALL = False
import datetime
import shelve

from hamilton.lifecycle.default import CacheAdapter

import molexp as me
from molexp.experiment import Experiment


class Project:
    def __init__(
        self,
        name: str,
        path: str | Path = Path.cwd(),
        tags: dict = {},
        description: str = "",
        proj_id: int | None = None,
    ):
        self.name = name
        self.description = description
        self._work_dir = Path(path).absolute() / name
        self.experiments = me.experiment.Experiments()
        self.assetes = []

        self.execution_manager = DefaultExecutionManager(
            executors.SynchronousLocalTaskExecutor(),
            executors.MultiThreadingExecutor(8),
        )
        self.tracker = None
        if SDK_INSTALL and proj_id:
            assert me.Config.USER_NAME != "undefined", ValueError(
                "Please set your username in molexp.Config.USER_NAME"
            )
            self.tracker = adapters.HamiltonTracker(
                project_id=proj_id,
                username=me.Config.USER_NAME,
                dag_name=name,
                tags=tags,
            )

    def __repr__(self):
        return f"<Project: {self.name}>"

    def add_asset(self, name: str) -> me.Asset:
        asset = me.Asset(name, self._work_dir / name)
        self.assetes.append(asset)
        return asset

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

    def start_task(
        self, path: str, param: me.Param, final_vars: list[str] = [], n_cases: int | None = None
    ):

        exp_name, task_name = path.split("/")
        exp = self.experiments.get_by_name(exp_name)
        task = exp.tasks.get_by_name(task_name)
        exp_tracker = exp.get_tracker(self._work_dir, n_cases=n_cases)  # inside proj path

        for case in range(n_cases):

            case_work_dir = exp_tracker.get_case_dir(case)

            task_tracker = task.get_tracker(case_work_dir)  # inside exp path
            modules = []
            modules.extend(exp.modules)
            modules.extend(task.modules)
            adapters = [exp_tracker, task_tracker]
            if self.tracker:
                adapters.append(self.tracker)

            cache_dir = task_tracker.work_dir / ".cache"
            cache_dir.mkdir(exist_ok=True)
            cache_adapter = CacheAdapter(cache_path=str(cache_dir / "cache"))
            adapters.append(cache_adapter)

            dr = self._get_driver(
                modules=modules,
                adapters=adapters,
            )

            if not final_vars:
                final_vars = dr.list_available_variables()

            _param = exp.get_param()
            _param.update(param)
            task.param = _param

            dr.execute(final_vars=final_vars, inputs=_param)

    def start_tasks(
        self,
        params: dict[str, me.Param],
        modules: list[ModuleType],
        final_vars: list[str] = [],
        n_cases: int | None = None,
    ):
        pass
