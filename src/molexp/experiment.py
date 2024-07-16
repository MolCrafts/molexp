from pathlib import Path
import datetime
from dataclasses import dataclass, asdict
from typing import Any, Dict, Optional
import hashlib
import inspect
import json
import logging
import os
from typing import Collection, List

from copy import deepcopy
from hamilton import graph_types, lifecycle

import molexp as me

logger = logging.getLogger(__name__)


class Task:

    def __init__(
        self,
        name: str,
        param: me.Param,
        modules: list,
        config: dict = {},
    ):
        self.name = name
        self.param = param
        self.config = config
        self.modules = modules

    def get_param(self):
        return self.param.copy()

    def get_config(self):
        return self.config

    def get_tracker(self, work_dir: str | Path = Path.cwd()):
        return TaskTracker(
            self.name,
            self.param,
            self.config,
            work_dir,
        )

    def start(self):
        pass

    def restart(self, param: me.Param | None = None):
        pass


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

    def def_task(self, name: str, param: me.Param, modules: list, config: dict = {}) -> Task:
        task = Task(
            name=name,
            param=param,
            modules=modules,
            config=config,
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


class Tasks(list):

    def get_by_name(self, name: str) -> Task:
        for task in self:
            if task.name == name:
                return task
        return None

    def add(self, task: Task):
        self.append(task)


class TaskTracker(
    lifecycle.NodeExecutionHook,
    # lifecycle.GraphExecutionHook,
    # lifecycle.GraphConstructionHook,
):
    def __init__(self, name: str, param: me.Param, config: dict, work_dir: str | Path):

        self.name = name
        self.param = param
        self.config = config

        self.init_dir = Path(work_dir)
        self.work_dir = Path(work_dir).resolve().joinpath(name)

        self.meta: dict = {}
        self.work_dir.mkdir(parents=True, exist_ok=True)

    def run_before_node_execution(
        self,
        *,
        node_name: str,
        node_tags: Dict[str, Any],
        node_kwargs: Dict[str, Any],
        node_return_type: type,
        task_id: Optional[str],
        run_id: str,
        node_input_types: Dict[str, Any],
        **future_kwargs: Any,
    ):
        os.chdir(self.work_dir)

    def run_after_node_execution(
        self,
        *,
        node_name: str,
        node_tags: Dict[str, Any],
        node_kwargs: Dict[str, Any],
        node_return_type: type,
        result: Any,
        error: Optional[Exception],
        success: bool,
        task_id: Optional[str],
        run_id: str,
        **future_kwargs: Any,
    ):
        os.chdir(self.init_dir)


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
