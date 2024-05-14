import dbm
from hamilton import driver
from hamilton.execution.grouping import TaskImplementation

# from hamilton.experimental.h_cache import CachingGraphAdapter

from hamilton.plugins import h_experiments
from hamilton.function_modifiers import value, parameterize, resolve, ResolveAt
from hamilton.execution.executors import DefaultExecutionManager, TaskExecutor
from hamilton.execution import executors

from rich.table import Table
from rich.console import Console

import json
from pathlib import Path

from .param import Param, ParamList
from hamilton import settings
from datetime import datetime

# ERROR: can not put parameterize function here,
#        it will cause "can not get lifecycle_name" error in NodeTransformLifecycle.__call__
# def materialize_exp(param: Param, root: Path, modules: list, materializers: list) -> str:
#     tracker = ExperimentTracker(param.name, root)
#     dr = (
#         driver.Builder()
#         .with_modules(*modules)
#         .enable_dynamic_execution(allow_experimental_mode=True)
#         .with_adapters(tracker)
#         .build()
#     )
#     dr.materialize(*materializers, inputs=param)
#     return tracker.run_id


@resolve(
    when=ResolveAt.CONFIG_AVAILABLE, decorate_with=lambda parameters: parameterize(**parameters)
)
def materialize_exp(
    param: Param, modules: list, materializers: list, root_dir: str, resume_path: Path | None
) -> str:
    tracker = h_experiments.ExperimentTracker(param.name, root_dir)
    if resume_path:
        # soft link all files in resume_path to tracker.run_directory
        for file in Path(resume_path).iterdir():
            (tracker.run_directory / file.name).symlink_to(file)

    dr = (
        driver.Builder()
        .with_modules(*modules)
        .enable_dynamic_execution(allow_experimental_mode=True)
        .with_adapters(tracker)
        .build()
    )
    param.update({"work_dir": tracker.run_directory})
    dr.materialize(*materializers, inputs=param)
    return tracker.run_id


class AllNodesRemoteExecutionManager(DefaultExecutionManager):
    def get_executor_for_task(self, task: TaskImplementation) -> TaskExecutor:
        """Simple implementation that returns the remote executor for all executions.
        :param task: Task to get executor for
        :return: Remote executor for all if applicable
        """
        is_single_node_task = len(task.nodes) == 1
        if not is_single_node_task:
            raise ValueError("Only single node tasks supported")
        return self.remote_executor
        # (node,) = task.nodes
        # if "cmdline" in node.tags:  # hard coded for now
        #     return self.remote_executor
        # return self.local_executor


class Project:
    def __init__(self, name: str, work_dir: str | Path = Path.cwd()):
        self.name = name
        self._root = Path(work_dir).absolute() / name
        # (Path(work_dir) / name).mkdir()
        self._root.mkdir(exist_ok=True)

        self.execution_manager = AllNodesRemoteExecutionManager(
            executors.SynchronousLocalTaskExecutor(),
            executors.MultiThreadingExecutor(8),
        )

    @property
    def root(self) -> Path:
        return self._root

    def run_exps(
        self,
        param_list: ParamList,
        materializers: list,
        /,
        *modules: list,
        resume: str | list | bool = False,
    ):

        config: dict[str, dict] = {"parameters": {}}
        for param in param_list:
            config["parameters"][param.name] = {
                "param": value(param),
                "modules": value(modules),
                "materializers": value(materializers),
                "root_dir": value(self.root),
                "resume_path": value(None),
            }

        if resume is True:
            # get latest run_id from each experiment
            items = self.list()
            exp_name = set([item["experiment"] for item in items])
            complete_time = {}
            resume_path = {}
            for name in exp_name:
                for item in filter(lambda x: (x["experiment"] == name and x["success"]), items):
                    if name not in resume_path:
                        complete_time[name] = datetime.fromisoformat(item["date_completed"])
                        resume_path[name] = item["run_dir"]
                    else:
                        if complete_time[name] < datetime.fromisoformat(item["date_completed"]):
                            complete_time[name] = datetime.fromisoformat(item["date_completed"])
                            resume_path[name] = item["run_dir"]
            for name in config["parameters"]:
                config["parameters"][name]["resume_path"] = value(resume_path[name])

        elif isinstance(resume, list):
            assert len(resume) == len(param_list)
            resume_path = {}
            exps = self.list()
            for param, res in zip(config["parameters"], resume):
                for exp in exps:
                    if exp["run_id"].startswith(res):
                        param["resume_path"] = exp["run_dir"]
            

        from molexp import project

        dr = (
            driver.Builder()
            .with_config(config)
            .with_modules(project)
            .enable_dynamic_execution(allow_experimental_mode=True)
            .with_execution_manager(self.execution_manager)
            .with_config({settings.ENABLE_POWER_USER_MODE: True})
            .build()
        )

        dr.execute(
            final_vars=[name for name in config["parameters"]],
        )

    def run_exp(self, param: Param, materializers: list, *modules: tuple):

        exp_path = self.root / param.name
        exp_path.mkdir(parents=True, exist_ok=True)
        param.update({"work_dir": exp_path})
        dr = (
            driver.Builder()
            .with_modules(*modules)
            .enable_dynamic_execution(allow_experimental_mode=True)
            .with_execution_manager(self.execution_manager)
            .build()
        )

        dr.materialize(*materializers, inputs=param)

        return

    def list(self, verbose: bool = False, console: bool = False) -> list[dict]:

        table = Table(title="Experiments")
        table.add_column("Name")
        table.add_column("Id")
        table.add_column("Success")
        table.add_column("Data_completed")
        table.add_column("Modules")
        table.add_column("Path")

        exp_list = []
        with dbm.open(str(self.root / "json_cache"), "r") as db:
            for key in db.keys():
                values = []
                exp = json.loads(db[key])
                values.append(exp["experiment"])
                values.append(exp["run_id"][:8])
                is_success = bool(exp["success"])
                values.append("✔" if is_success else "✘")
                if is_success:
                    values.append(exp["date_completed"])
                else:
                    values.append("\\")
                values.append(",".join(exp["modules"]))
                values.append(exp["run_dir"])
                table.add_row(*values)
                exp_list.append(exp)

        if console:
            console = Console()
            console.print(table)

        return exp_list
