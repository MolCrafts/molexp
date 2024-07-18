from types import ModuleType
import molexp as me
from hamilton_sdk import adapters
from hamilton.execution.executors import DefaultExecutionManager
from hamilton.execution import executors
from hamilton import driver
from hamilton.lifecycle import GraphAdapter
from hamilton.experimental import h_cache

from pathlib import Path
from typing import Any

from molexp.experiment import Experiment
import os


# def map_func(
#     tasks: ParamList|list[me.Task],
#     experiments: list[Experiment],
#     proj_dir: Path,
#     modules: tuple,
#     materializers:list=[],
#     additional_vars:list=[],
# ) -> Parallelizable[Any]:
#     for task in tasks:
#         for experiment in experiments:
#             param = experiment.get_param()
#             param.update(task.get_param())

#             experiment_tracker = experiment.get_tracker(proj_dir)
#             task_tracker = task.get_tracker(experiment_tracker.work_dir)

#             config = task.get_config()
#             config.update(experiment.get_config())

#             trackers = [experiment_tracker, task_tracker]
#             dr = (
#                 driver.Builder()
#                 .with_modules(*modules)
#                 .with_config(task.get_config())
#                 .with_adapters(*trackers)
#                 .build()
#             )

#             if not materializers or not additional_vars:
#                 additional_vars = dr.list_available_variables()
#             yield dr.materialize(*materializers, inputs=param, additional_vars=additional_vars)


# def reduce_func(map_func: Parallelizable[Any]) -> Any:
#     pass


class Project:
    def __init__(
        self,
        name: str,
        work_dir: str | Path = Path.cwd(),
        tags: dict = {},
        description: str = "",
        proj_id: int | None = None,
    ):
        self.name = name
        self._work_dir = Path(work_dir).absolute() / name
        self._work_dir.mkdir(exist_ok=True)
        self.description = description

        self.execution_manager = DefaultExecutionManager(
            executors.SynchronousLocalTaskExecutor(),
            executors.MultiThreadingExecutor(8),
        )
        self.experiments = me.experiment.Experiments()
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
    def work_dir(self) -> Path:
        return self._work_dir
    
    def def_exp(self, name:str, param:me.Param):
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

        dr = (
            driver.Builder()
            .with_modules(*modules)
            .with_adapters(*adapters)
            .build()
        )
        return dr
    
    def start_task(self, path: str, final_vars: list[str] = []):
        
        exp_name, task_name = path.split("/")
        exp = self.experiments.get_by_name(exp_name)
        task = exp.tasks.get_by_name(task_name)
        exp_tracker = exp.get_tracker(self._work_dir)  # inside proj path
        task_tracker = task.get_tracker(exp_tracker.work_dir)  # inside exp path
        modules = []
        modules.extend(exp.modules)
        modules.extend(task.modules)
        adapters = [exp_tracker, task_tracker]
        if self.tracker:
            adapters.append(self.tracker)

        cache_dir = task_tracker.work_dir/'.cache'
        cache_dir.mkdir(exist_ok=True)
        cache_adapter = h_cache.CachingGraphAdapter(cache_dir)
        adapters.append(cache_adapter)

        dr = self._get_driver(
            modules=modules,
            adapters=adapters,
        )

        if not final_vars:
            final_vars = dr.list_available_variables()

        params = exp.get_param()
        params.update(task.get_param())

        return dr.execute(
            final_vars=final_vars,
            inputs=params
        )

    # def resume_task(self, name:str, param:me.Param, modules: list[ModuleType] = [], config:dict={}, from_task:str|None=None, from_files: list[str|Path]|None = None, final_vars: list[str] = []):
        
    #     exp_name, task_name = name.split("/")
    #     exp = self.experiments.get_by_name(exp_name)
    #     task = exp.def_task(task_name, param, modules, config)
    #     exp_tracker = exp.get_tracker(self._work_dir)
    #     task_tracker = task.get_tracker(exp_tracker.work_dir)

    #     if (from_files is None and from_task is None) or (from_files and from_task):
    #         raise ValueError("Either from_task or from_files should be provided")
    #     if from_task:
    #         exp_name, task_name = from_task.split("/")
    #         exp = self.experiments.get_by_name(exp_name)
    #         from_task_obj = exp.tasks.get_by_name(task_name)
    #         exp_tracker = exp.get_tracker(self._work_dir)
    #         from_task_tracker = from_task_obj.get_tracker(exp_tracker.work_dir)
    #         from_task_path = from_task_tracker.work_dir
    #         for file in from_task_path.iterdir():
    #             to_path = task_tracker.work_dir / file.name
    #             if to_path.exists():
    #                 to_path.unlink()
    #             to_path.symlink_to(file)
    #     elif from_files:
            
    #         for file_path in from_files:
    #             Path(file_path).symlink_to(task_tracker.work_dir / Path(file_path).name)
                

    #     self.start_task(name, final_vars)
        

    # def run(self, task, experiment, *modules, materializers=[], additional_vars=[]):

    #     param = experiment.get_param()
    #     param.update(task.get_param())

    #     experiment_tracker = experiment.get_tracker(self.work_dir)
    #     task_tracker = task.get_tracker(experiment_tracker.work_dir)

    #     config = task.get_config()
    #     config.update(experiment.get_config())

    #     trackers = [task_tracker, experiment_tracker]
    #     if self.tracker:
    #         trackers.append(self.tracker)

    #     trackers.append(
    #         h_cache.CachingGraphAdapter(experiment_tracker.work_dir / ".cache")
    #     )
    #     (experiment_tracker.work_dir / ".cache").mkdir(exist_ok=True, parents=True)

    #     param.update({
    #         'task_dir': task_tracker.work_dir,
    #         'exp_dir': experiment_tracker.work_dir,
    #         'proj_dir': self.work_dir,
    #     })

    #     dr = (
    #         driver.Builder()
    #         .with_modules(*modules)
    #         .with_config(task.get_config())
    #         .with_adapters(*trackers)
    #         .build()
    #     )

    #     if not materializers and not additional_vars:
    #         additional_vars = dr.list_available_variables()
    #     print(additional_vars)
    #     dr.materialize(*materializers, inputs=param, additional_vars=additional_vars)

    #     os.chdir(Path.cwd())

    # def batch_run(self, tasks, experiments, *modules, materializers=[], additional_vars=[]):

    #     trackers = []
    #     if self.tracker:
    #         trackers.append(self.tracker)

    #     import molexp.project
    #     dr = (
    #         driver.Builder()
    #         .with_modules(molexp.project)
    #         .with_config({})
    #         .enable_dynamic_execution(allow_experimental_mode=True)
    #         .with_adapters(self.tracker)
    #         .build()
    #     )

    #     dr.execute(
    #         dr.list_available_variables(),
    #         inputs={
    #             "tasks": tasks,
    #             "experiments": experiments,
    #             "modules": modules,
    #             "proj_dir": self.work_dir,
    #             "materializers": materializers,
    #             "additional_vars": additional_vars,
    #         },
    #     )

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
    #         base_directory=self.work_dir,
    #     )

    #     config.update(
    #         {
    #             "proj_dir": self._work_dir,
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
    #     with dbm.open(str(self.work_dir / "json_cache"), "r") as db:
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

    #     with dbm.open(str(self.work_dir / "json_cache"), "r") as db:
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
