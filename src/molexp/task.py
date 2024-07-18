from hamilton import lifecycle
from molexp.param import Param
from pathlib import Path
from typing import Any
import os, types


class Task:

    def __init__(
        self,
        name: str,
        param: Param = Param(),
        modules: list[types.ModuleType] = [],
        config: dict = {},
        dep_files: list[str] = [],
    ):
        self.name = name
        self.param = param
        self.config = config
        self.modules = modules
        self.dep_files = dep_files

    @classmethod
    def union(cls, name: str, *tasks: "Task") -> "Task":
        param = Param()
        for task in tasks:
            param |= task.param
        return cls(name, param)

    def __repr__(self):
        return f"<Task: {self.name}>"

    def get_param(self):
        return self.param.copy()

    def get_config(self):
        return self.config

    def get_tracker(self, work_dir: str | Path = Path.cwd()):
        return TaskTracker(self.name, self.param, self.config, work_dir, self.dep_files)

    def start(self):
        pass

    def restart(self, param: Param | None = None):
        pass


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
        param: Param,
        config: dict,
        work_dir: str | Path,
        dep_files: list[str],
    ):

        self.name = name
        self.param = param
        self.config = config
        self.dep_files = dep_files

        self.init_dir = Path(work_dir)
        self.work_dir = Path(work_dir).resolve().joinpath(name)

        self.meta: dict = {}
        self.work_dir.mkdir(parents=True, exist_ok=True)

        for dep in dep_files:
            task_name, file_name = dep.split("/")
            task_path = self.init_dir / task_name
            if not task_path.exists():
                raise FileNotFoundError(f"Task {task_name} not found.")

            for file in task_path.glob(file_name):
                file_link = self.work_dir / file.name
                if file_link.exists():
                    file_link.unlink()
                file_link.symlink_to(file)

    def run_before_node_execution(
        self,
        *,
        node_name: str,
        node_tags: dict[str, Any],
        node_kwargs: dict[str, Any],
        node_return_type: type,
        task_id: str | None,
        run_id: str,
        node_input_types: dict[str, Any],
        **future_kwargs: Any,
    ):
        os.chdir(self.work_dir)

    def run_after_node_execution(
        self,
        *,
        node_name: str,
        node_tags: dict[str, Any],
        node_kwargs: dict[str, Any],
        node_return_type: type,
        result: Any,
        error: Exception | None,
        success: bool,
        task_id: str | None,
        run_id: str,
        **future_kwargs: Any,
    ):
        os.chdir(self.init_dir)
