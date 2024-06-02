from types import ModuleType
import molexp as me
import os

from hamilton import driver
from hamilton_sdk import adapters
from hamilton.execution.executors import DefaultExecutionManager
from hamilton.execution import executors
from hamilton.htypes import Parallelizable

from pathlib import Path
from typing import Any

from molexp.experiment import Experiment
from molexp.param import ParamList


def map_func(
    tasks: ParamList|list[me.Task],
    experiments: list[Experiment],
    proj_dir: Path,
    modules: tuple,
    materializers:list=[],
    additional_vars:list=[],
) -> Parallelizable[Any]:
    for task in tasks:
        for experiment in experiments:
            param = experiment.get_param()
            param.update(task.get_param())

            experiment_tracker = experiment.get_tracker(proj_dir)
            task_tracker = task.get_tracker(experiment_tracker.work_dir)

            config = task.get_config()
            config.update(experiment.get_config())

            trackers = [experiment_tracker, task_tracker]
            dr = (
                driver.Builder()
                .with_modules(*modules)
                .with_config(task.get_config())
                .with_adapters(*trackers)
                .build()
            )

            if not materializers or not additional_vars:
                additional_vars = dr.list_available_variables()
            yield dr.materialize(*materializers, inputs=param, additional_vars=additional_vars)


def reduce_func(map_func: Parallelizable[Any]) -> Any:
    pass


class Project:
    def __init__(
        self,
        name: str,
        work_dir: str | Path = Path.cwd(),
        tags: dict = {},
        header: dict[str, str] = {},
        proj_id: int | None = None,
    ):
        self.name = name
        self._root = Path(work_dir).absolute() / name
        self._root.mkdir(exist_ok=True)

        self.execution_manager = DefaultExecutionManager(
            executors.SynchronousLocalTaskExecutor(),
            executors.MultiThreadingExecutor(8),
        )
        self.header = header
        self.experiments = []

        self.tracker = None
        if proj_id:
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
    def root(self) -> Path:
        return self._root

    def run(self, task, experiment, *modules, materializers=[], additional_vars=[]):

        param = experiment.get_param()
        param.update(task.get_param())

        experiment_tracker = experiment.get_tracker(self.root)
        task_tracker = task.get_tracker(experiment_tracker.work_dir)

        config = task.get_config()
        config.update(experiment.get_config())

        trackers = [task_tracker, experiment_tracker]
        if self.tracker:
            trackers.append(self.tracker)

        dr = (
            driver.Builder()
            .with_modules(*modules)
            .with_config(task.get_config())
            .with_adapters(*trackers)
            .build()
        )

        if not materializers or not additional_vars:
            additional_vars = dr.list_available_variables()
        dr.materialize(*materializers, inputs=param, additional_vars=additional_vars)

        os.chdir(Path.cwd())

    def batch_run(self, tasks, experiments, *modules, materializers=[], additional_vars=[]):

        trackers = []
        if self.tracker:
            trackers.append(self.tracker)

        import molexp.project
        dr = (
            driver.Builder()
            .with_modules(molexp.project)
            .with_config({})
            .enable_dynamic_execution(allow_experimental_mode=True)
            .with_adapters(self.tracker)
            .build()
        )

        dr.execute(
            dr.list_available_variables(),
            inputs={
                "tasks": tasks,
                "experiments": experiments,
                "modules": modules,
                "proj_dir": self.root,
                "materializers": materializers,
                "additional_vars": additional_vars,
            },
        )

    # def run_exp(
    #     self,
    #     name: str,
    #     param: me.Param,
    #     *modules: tuple,
    #     materializers: list = [],
    #     additional_vars=[],
    #     config: dict = {},
    # ):

    #     param.update(self.header)

    #     self.exp_tracker = ExperimentTracker(
    #         run_name=name,
    #         exp_name=param.name,
    #         base_directory=self.root,
    #     )

    #     config.update(
    #         {
    #             "proj_dir": self._root,
    #             "exp_dir": self.exp_tracker.run_directory,
    #         }
    #     )

    #     experiment = self.exp_tracker.get_experiment()
    #     self.add_experiment(experiment)

    #     trackers = [self.exp_tracker]
    #     if self.tracker:
    #         trackers.append(self.tracker)

    #     dr = (
    #         driver.Builder()
    #         .with_modules(*modules)
    #         .with_config(config)
    #         .with_adapters(*trackers)
    #         .build()
    #     )

    #     dr.materialize(*materializers, inputs=param, additional_vars=additional_vars)

    #     return self

    # def ls(self, verbose: bool = False, console: bool = False) -> list[dict]:

    #     table = Table(title="Experiments")
    #     table.add_column("Name")
    #     table.add_column("Id")
    #     table.add_column("Success")
    #     table.add_column("Data_completed")
    #     table.add_column("Modules")
    #     table.add_column("Path")

    #     exp_list = []
    #     with dbm.open(str(self.root / "json_cache"), "r") as db:
    #         for key in db.keys():
    #             values = []
    #             exp = json.loads(db[key])
    #             values.append(exp["experiment"])
    #             values.append(exp["run_id"][:8])
    #             is_success = bool(exp["success"])
    #             values.append("✔" if is_success else "✘")
    #             if is_success:
    #                 values.append(exp["date_completed"])
    #             else:
    #                 values.append("\\")
    #             values.append(",".join(exp["modules"]))
    #             values.append(exp["run_dir"])
    #             table.add_row(*values)
    #             exp_list.append(exp)

    #     if console:
    #         console = Console()
    #         console.print(table)

    #     return exp_list

    # def get_experiments(self) -> list[ExpInfo]:

    #     with dbm.open(str(self.root / "json_cache"), "r") as db:
    #         run_data_list = [json.loads(db[key]) for key in db.keys()]

    #     exp_list = []
    #     for run_data in run_data_list:
    #         exp_list.append(ExpInfo(run_data, None))
    #     return exp_list

    # def query_experiments(self, filter_fn: Callable) -> list[ExpInfo]:

    #     exp_list = self.get_experiments()
    #     candidates = sorted(
    #         filter(filter_fn, exp_list),
    #         key=lambda exp_info: datetime.fromisoformat(exp_info.run_data["date_completed"]),
    #     )
    #     return candidates
