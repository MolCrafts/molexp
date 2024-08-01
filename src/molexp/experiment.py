import os
import types
from pathlib import Path
from typing import Any, Collection, Dict, List

from hamilton import graph_types, lifecycle

from molexp.param import Param
from molexp.task import Task, Tasks
from molexp.asset import Asset


class Experiment:

    def __init__(
        self,
        name: str,
        param: Param = Param(),
        config: dict = {},
        path: Path = Path.cwd(),
    ):
        self.name = name
        self.path = Path(path)
        self.param = param
        self.config = config
        self.tasks = Tasks()
        self._n_trials = 0

        self._modules = []

        self._work_dir = self.path / name

    @property
    def n_trials(self):
        return self._n_trials

    def init(self, n_trials: int=0):
        if not self.path.exists():
            self.path.mkdir(parents=True, exist_ok=True)
        self._n_trials = n_trials
        if n_trials:
            for i in range(n_trials):
                trial_dir = self.path / f"trial{i}"
                if not trial_dir.exists():
                    trial_dir.mkdir(parents=True, exist_ok=True)

    def __repr__(self):
        return f"<Experiment: {self.name}>"
    
    def __call__(self, name: str|None = None, path: str|None = None, param: Param|None = None, config: dict|None = None):

        name = name or self.name
        path = path or self.path
        param = param or self.param
        config = config or self.config

        return Experiment(
            name=name,
            path=path,
            param=param,
            config=config,
        )
        
    
    def add_asset(self, name: str)->Asset:
        return Asset(name, self._work_dir / name)

    @property
    def modules(self):
        return self._modules

    @property
    def work_dir(self) -> Path:
        return self._work_dir

    def get_param(self):
        return self.param.copy()

    def get_config(self):
        return self.config

    def get_tracker(self, work_dir: str | Path = Path.cwd(), n_cases: int|None = None):
        return ExperimentTracker(
            self.name,
            self.param,
            self.config,
            work_dir,
            n_cases,
        )

    def get_task(self, name: str) -> Task:
        return self.tasks.get_by_name(name)

    def def_task(
        self,
        name: str,
        modules: list[types.ModuleType] = [],
        config: dict = {},
        dep_files: list[str] = [],
    ) -> Task:
        task = Task(name=name, path=self.work_dir, param=None, modules=modules, config=config, dep_files=dep_files)
        task.init()
        self.tasks.add(task)
        return task
    
    def merge_task(
        self,
        tasks_to_merge: list[str],
    ):
        tasks = [self.get_task(task) for task in tasks_to_merge]
        for task in tasks:
            self.tasks.remove(task)

        task = Task.union(self.name, *tasks)
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

    @classmethod
    def load(cls, metadata: dict):
        exps = cls()
        for exp_name, exp_meta in metadata.items():
            exp = Experiment.load(exp_meta)
            exps.add(exp)
        return exps


class ExperimentTracker(
    # lifecycle.NodeExecutionHook,
    lifecycle.GraphExecutionHook,
    # lifecycle.GraphConstructionHook,
):
    def __init__(self, name: str, param: Param, config: dict, work_dir: str | Path, n_cases: int|None = None):

        self.name = name
        self.param = param
        self.config = config

        self.init_dir = Path(work_dir)
        self.work_dir = Path(work_dir).resolve().joinpath(name)

        self.n_cases = n_cases

        if not self.work_dir.exists():
            self.work_dir.mkdir(parents=True, exist_ok=True)
        
        if self.n_cases:
            for i in range(self.n_cases):
                case_dir = self.work_dir / f"case{i}"
                if not case_dir.exists():
                    case_dir.mkdir(parents=True, exist_ok=True)

    def get_case_dir(self, case: int):
        return self.work_dir / f"case{case}"

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
