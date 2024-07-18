from pathlib import Path
from typing import Any, Dict
import os
from typing import Collection, List

from hamilton import graph_types, lifecycle

import molexp as me
from molexp.task import Task, Tasks

class Experiment:

    def __init__(
        self,
        name: str,
        param: me.Param,
        config: dict = {},
    ):
        self.name = name
        self.param = param
        self.config = config
        self.tasks = Tasks()

        self._modules = []

    def __repr__(self):
        return f"<Experiment: {self.name}>"

    @property
    def modules(self):
        return self._modules

    def get_param(self):
        return self.param.copy()

    def get_config(self):
        return self.config

    def get_tracker(self, work_dir: str | Path = Path.cwd()):
        return ExperimentTracker(
            self.name,
            self.param,
            self.config,
            work_dir,
        )

    def get_task(self, name: str) -> Task:
        return self.tasks.get_by_name(name)

    def def_task(self, name: str, param: me.Param, modules: list, config: dict = {}) -> Task:
        task = Task(
            name=name,
            param=param,
            modules=modules,
            config=config,
        )
        self.tasks.add(task)
        return task

    def resume_task(
        self,
        name: str,
        param: me.Param,
        modules: list,
        config: dict = {},
        from_files: list[str] | None = None,
    ) -> Task:

        task = Task(
            name=name,
            param=param,
            modules=modules,
            config=config,
            dependencies=from_files,
        )
        self.tasks.add(task)
        return task

    def ls(self):
        return self.tasks


class Experiments(list):

    def get_by_name(self, name: str) -> Experiment:
        for exp in self:
            if exp.name == name:
                return exp
        return None

    def add(self, exp: Experiment):
        self.append(exp)


class ExperimentTracker(
    # lifecycle.NodeExecutionHook,
    lifecycle.GraphExecutionHook,
    # lifecycle.GraphConstructionHook,
):
    def __init__(self, name: str, param: me.Param, config: dict, work_dir: str | Path):

        self.name = name
        self.param = param
        self.config = config

        self.init_dir = Path(work_dir)
        self.work_dir = Path(work_dir).resolve().joinpath(name)

        self.meta: dict = {}
        if not self.work_dir.exists():
            self.work_dir.mkdir(parents=True, exist_ok=True)

    def run_before_graph_execution(
        self,
        *,
        graph: graph_types.HamiltonGraph,
        final_vars: List[str],
        inputs: Dict[str, Any],
        overrides: Dict[str, Any],
        execution_path: Collection[str],
        run_id: str,
        **future_kwargs: Any,
    ):
        os.chdir(self.work_dir)

    def run_after_graph_execution(
        self,
        *,
        graph: graph_types.HamiltonGraph,
        success: bool,
        error: Exception | None,
        results: Dict[str, Any] | None,
        run_id: str,
        **future_kwargs: Any,
    ):
        os.chdir(self.init_dir)
