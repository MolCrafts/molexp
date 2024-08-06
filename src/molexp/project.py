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
from typing import Iterable, Callable, Any


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

    def def_exp(self, name: str):
        exp = Experiment(
            name=name,
            path=self._work_dir,
        )
        exp.init()
        self.add_exp(exp)
        return exp

    def add_exp(self, exp: Experiment):
        self.experiments.add(exp)

    def get_exp(self, name: str) -> Experiment:
        return self.experiments.get_by_name(name)

    def get_tasks(self, path: str) -> list[me.Task]:
        path_list = reversed(path.split("/"))

        exp = self.get_exp(path_list.pop())
        tasks = exp.get_task(path_list)

        return tasks

    def ls(self):
        return self.experiments

    def _get_driver(
        self, task: me.Task, config: dict, exp: me.Experiment | None = None
    ) -> driver.Driver:

        modules = set()

        modules.update(set(task.modules))
        task_tracker = task.get_tracker()
        adapters = [task_tracker]
        if exp is not None:
            exp_tracker = exp.get_tracker()
            adapters.append(exp_tracker)
            modules.update(set(exp.modules))
        cache_dir = task_tracker.work_dir / ".cache"
        cache_dir.mkdir(exist_ok=True)
        cache_adapter = CacheAdapter(cache_path=str(cache_dir / "cache"))
        adapters.append(cache_adapter)
        dr = (
            driver.Builder()
            .with_adapters(*adapters)
            .with_modules(*modules)
            .with_config(config)
            .build()
        )

        return dr

    def start_task(
        self,
        task: me.Task,
        param: me.Param = me.Param(),
        final_vars: list[str] = [],
        config: dict = {},
    ):

        dr = self._get_driver(task, config)
        if not final_vars:
            final_vars = dr.list_available_variables()

        task.param |= param
        dr.execute(final_vars=final_vars, inputs=task.param)

    def restart_task(
        self, task: me.Task, param: me.Param, final_vars: list[str] = [], config: dict = {}
    ):

        # clean up the cache
        cache_dir = task.get_tracker().work_dir / ".cache"
        cache_dir.unlink()

        self.start_task(task, param, final_vars, config)

    def start_tasks(
        self,
        tasks: list[me.Task],
        params: list[me.Param] = [],
        final_vars: list[str] = [],
        config: dict = {},
        reducer_fn: Callable = lambda x: x,
    ):
        from hamilton.htypes import Parallelizable, Collect
        from hamilton import ad_hoc_utils
        from hamilton.execution.executors import ExecutionManager

        if not params:
            params = [me.Param() for _ in range(len(tasks))]

        def mapper(tasks: list[me.Task], params: list[me.Param]) -> Parallelizable[Any]:
            for task, param in zip(tasks, params):
                dr = self._get_driver(task, config)
                task.param |= param
                yield {"dr": dr, "inputs": task.param, "final_vars": final_vars}
                # yield dr.execute(inputs=task.param, final_vars=final_vars)

        def dag_result(mapper: dict) -> dict:
            _dr = mapper["dr"]
            _inputs = mapper["inputs"]
            _final_vars = mapper["final_vars"]
            return _dr.execute(inputs=_inputs, final_vars=_final_vars)

        def reducer(dag_result: Collect[dict]) -> Any:

            return reducer_fn(dag_result)

        temp_module = ad_hoc_utils.create_temporary_module(
            mapper,
            dag_result,
            reducer,
            module_name="start_tasks_mapper_reducer",
        )

        dr = (
            driver.Builder()
            .with_modules(temp_module)
            .enable_dynamic_execution(allow_experimental_mode=True)
            .with_remote_executor(executors.MultiThreadingExecutor(8))
            .build()
        )
        dr.execute(final_vars=["reducer"], inputs={"tasks": tasks, "params": params})
