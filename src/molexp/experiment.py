from pathlib import Path
from typing import Any, Dict, Optional
import logging
import os
from typing import Collection, List

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
        dependencies: list[str] = [],
    ):
        self.name = name
        self.param = param
        self.config = config
        self.modules = modules
        self.dependencies = dependencies

    def __repr__(self):
        return f"<Task: {self.name}>"

    def get_param(self):
        return self.param.copy()

    def get_config(self):
        return self.config

    def get_tracker(self, work_dir: str | Path = Path.cwd()):
        return TaskTracker(self.name, self.param, self.config, work_dir, self.dependencies)

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
    def __init__(
        self,
        name: str,
        param: me.Param,
        config: dict,
        work_dir: str | Path,
        dependencies: list[str],
    ):

        self.name = name
        self.param = param
        self.config = config
        self.dependencies = dependencies

        self.init_dir = Path(work_dir)
        self.work_dir = Path(work_dir).resolve().joinpath(name)

        self.meta: dict = {}
        self.work_dir.mkdir(parents=True, exist_ok=True)

        for dep in dependencies:
            task_name, file_name = dep.split('/')
            task_path = self.init_dir / task_name
            if not task_path.exists():
                raise FileNotFoundError(f"Task {task_name} not found.")
            
            for file in task_path.glob(file_name):
                if not file.exists():
                    raise FileNotFoundError(f"File {file_name} not found in task {task_name}.")
                file_link = self.work_dir / file.name
                file_link.symlink_to(file)

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
