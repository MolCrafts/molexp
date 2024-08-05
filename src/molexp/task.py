from hamilton import lifecycle
from molexp.param import Param
from pathlib import Path
from typing import Any
import os, types
from enum import IntEnum


class Task:

    class Status(IntEnum):
        PENDING = 0
        RUNNING = 1
        COMPLETED = 2
        Error = 3

    def __init__(
        self,
        name: str,
        param: Param = Param(),
        modules: list[types.ModuleType] = [],
        config: dict = {},
        dep_files: list[str] = [],
        path: Path = Path.cwd(),
    ):
        self.name = name
        self.path = Path(path)
        self.param = param
        self.config = config
        self.modules = modules
        self.dep_files = dep_files

        self.work_dir = self.path / self.name

    def init(self):
        self.work_dir.mkdir(parents=True, exist_ok=True)

    @classmethod
    def load(cls, state: dict):
        ins = cls(
            state["name"],
            state["path"],
            state["param"],
            state["config"],
            state["modules"],
            state["dep_files"],
        )
        ins.init()
        return cls

    def __getstate__(self):
        return self.get_state()

    def __setstate__(self, state):
        self.__dict__.update(state)

    def get_state(self):
        return {
            "name": self.name,
            "param": self.param,
            "config": self.config,
            "modules": self.modules,
            "dep_files": self.dep_files,
        }

    @classmethod
    def union(cls, name: str, *tasks: "Task") -> "Task":
        param = Param()
        config = {}
        modules = []
        dep_files = []
        for task in tasks:
            param |= task.param
            config |= task.config
            modules += task.modules
            dep_files += task.dep_files
        return cls(
            name=name,
            path=tasks[0].path,
            param=param,
            modules=modules,
            config=config,
            dep_files=dep_files,
        )

    def __repr__(self):
        return f"<Task: {self.name}>"

    def __or__(self, other: "Task") -> "Task":
        return Task.union(self.name, self, other)

    def get_param(self):
        return self.param.copy()

    def get_config(self):
        return self.config

    def get_tracker(self):
        return TaskTracker(self.name, self.param, self.config, self.work_dir, self.dep_files)


class Tasks(list):

    def get_by_name(self, name: str) -> Task:
        for task in self:
            if task.name == name:
                return task
        return None

    def add(self, task: Task):
        if task in self:
            return self.get_by_name(task.name) | task
        else:
            self.append(task)

    @classmethod
    def load(cls, metadata: dict):
        tasks = cls()
        for task_name, task_metadata in metadata.items():
            task = Task.load(task_metadata)
            tasks.add(task)
        return tasks


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

        self.work_dir = Path(work_dir)

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
        self.init_dir = Path.cwd()
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
