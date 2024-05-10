import datetime
import json
import logging
import os
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Callable

from hamilton import graph_types, lifecycle, base
from hamilton.plugins.h_experiments.cache import JsonCache

logger = logging.getLogger(__name__)


from .utils import validate_string_input, _get_default_input


@dataclass
class NodeImplementation:
    name: str
    source_code: str


@dataclass
class NodeInput:
    name: str
    value: Any
    default_value: Optional[Any]


@dataclass
class NodeOverride:
    name: str
    value: Any


@dataclass
class NodeMaterializer:
    source_nodes: list[str]
    path: str
    sink: str
    data_saver: str


@dataclass
class RunMetadata:
    """Metadata about an Hamilton to store in cache"""

    experiment: str
    run_id: str
    run_dir: str
    success: bool
    date_completed: datetime.datetime
    graph_hash: str
    modules: list[str]
    config: dict
    inputs: list[NodeInput]
    overrides: list[NodeOverride]
    materialized: list[NodeMaterializer]


class ExperimentTracker(
    lifecycle.NodeExecutionHook,
    lifecycle.GraphExecutionHook,
    lifecycle.GraphConstructionHook,
):
    def __init__(self, base_directory: str = "./experiments", is_exec_in_subdir:bool=True):
        # validate_string_input(experiment_name)

        # self.experiment_name = experiment_name
        self.base_directory = base_directory
        self.cache = JsonCache(cache_path=base_directory)
        # self.run_id = str(uuid.uuid4())

        self.init_directory = Path.cwd()
        # self.run_directory = (
        #     Path(base_directory).resolve().joinpath(self.experiment_name, self.run_id)
        # )
        # self.run_directory.mkdir(exist_ok=True, parents=True)

        self.is_exec_in_subdir = is_exec_in_subdir

        self.graph_hash: str = ""
        self.modules: set[str] = set()
        self.config = dict()
        self.inputs: list[NodeInput] = list()
        self.overrides: list[NodeOverride] = list()
        self.materializers: list[NodeMaterializer] = list()

    def init_experiment(self, experiment_name: str):
        validate_string_input(experiment_name)
        self.experiment_name = experiment_name
        self.run_id = str(uuid.uuid4())
        self.run_directory = (
            Path(self.base_directory).resolve().joinpath(self.experiment_name, self.run_id)
        )
        self.run_directory.mkdir(exist_ok=True, parents=True)

    def run_after_graph_construction(self, *, config: dict[str, Any], **kwargs):
        """Store the Driver config before creating the graph"""
        self.config = config

    def run_before_graph_execution(
        self,
        *,
        graph: graph_types.HamiltonGraph,
        inputs: Dict[str, Any],
        overrides: Dict[str, Any],
        **kwargs,
    ):
        """Store execution metadata: graph hash, inputs, overrides"""
        self.graph_hash = graph.version
        if self.is_exec_in_subdir:
            os.chdir(self.run_directory)

        for node in graph.nodes:
            if node.tags.get("module"):
                self.modules.add(node.tags["module"])

            # filter out config nodes
            elif node.is_external_input and node.originating_functions:
                self.inputs.append(
                    NodeInput(
                        name=node.name,
                        value=inputs.get(node.name),
                        default_value=_get_default_input(node),
                    )
                )

        if overrides:
            self.overrides = [NodeOverride(name=k, value=v) for k, v in overrides.items()]

    def run_before_node_execution(self, *args, node_tags: dict, **kwargs):
        """Move to run directory before executing materializer"""
        if node_tags.get("hamilton.data_saver") is True and self.is_exec_in_subdir is False:
            os.chdir(self.run_directory)  # before materialization

    def run_after_node_execution(
        self, *, node_name: str, node_tags: dict, node_kwargs: dict, result: Any, **kwargs
    ):
        """Move back to init directory after executing materializer.
        Then, save materialization metadata
        """
        if node_tags.get("hamilton.data_saver") is True and self.is_exec_in_subdir is False:
            if "path" in result:
                path = result["path"]
            elif "file_metadata" in result:
                path = result["file_metadata"]["path"]
            else:
                logger.warning(
                    f"Materialization result from node={node_name} has no recordable path: {result}. Materializer must have either "
                    f"'path' or 'file_metadata' keys."
                )
            self.materializers.append(
                NodeMaterializer(
                    source_nodes=list(node_kwargs.keys()),
                    path=str(Path(path).resolve()),
                    sink=node_tags["hamilton.data_saver.sink"],
                    data_saver=node_tags["hamilton.data_saver.classname"],
                )
            )
            os.chdir(self.init_directory)  # after materialization

    def run_after_graph_execution(self, *, success: bool, **kwargs):
        """Encode run metadata as JSON and store in cache"""
        run_data = dict(
            experiment=self.experiment_name,
            run_id=self.run_id,
            run_dir=str(self.run_directory),
            date_completed=datetime.datetime.now().isoformat(),
            success=success,
            graph_hash=self.graph_hash,
            modules=list(self.modules),
            config=self.config,
            inputs=[] if len(self.inputs) == 0 else [asdict(i) for i in self.inputs],
            overrides=[] if len(self.overrides) == 0 else [asdict(o) for o in self.overrides],
            materialized=[asdict(m) for m in self.materializers],
        )

        run_json_string = json.dumps(run_data, default=str, sort_keys=True)
        self.cache.write(run_json_string, self.run_id)
        if self.is_exec_in_subdir:
            os.chdir(self.init_directory)

from hamilton import driver, settings

# from hamilton.plugins import h_experiments
from hamilton.function_modifiers import value, parameterize_extract_columns, ParameterizedExtract, resolve, ResolveAt
from hamilton.execution.executors import DefaultExecutionManager
from hamilton.execution import executors
from hamilton.io.materialization import to

import os
from pathlib import Path
from .param import ParamList, Param


class Experiment(dict):

    @classmethod
    def from_cache(cls, cache: dict):
        return cls(cache)

    @property
    def param(self)->Param:
        return Param({input_["name"]: input_["value"] for input_ in self["inputs"]})
    
    @property
    def name(self):
        return self["experiment"]
    
    def __repr__(self):
        return f"Experiment({self.name})"

class ExperimentGroup(list[Experiment]):

    @property
    def params(self)->ParamList:
        param_list = ParamList()
        for exp in self:
            param_list.append(exp.param)

        return param_list

    def map_reduce(self, materializers: list, reducer:Callable=lambda x: x, *modules:list)->dict[Experiment, Any]:
        root = Path.cwd()
        execution_manager = DefaultExecutionManager(
            executors.SynchronousLocalTaskExecutor(),
            executors.MultiThreadingExecutor(20),
        )

        parameters = {exp.name: {"exp": value(exp)} for exp in self}

        @resolve(when=ResolveAt.CONFIG_AVAILABLE, decorate_with=lambda: parameterize_extract_columns(*[ParameterizedExtract(tuple(exp.name), {"exp": value(exp)}) for exp in self]))
        def mapper(exp:Experiment) -> dict:
            os.chdir(exp['run_dir'])
            dr = (
                driver.Builder()
                .with_modules(*modules)
                .build()
            )
            result = dr.materialize(*materializers, inputs=exp.param)
            # result = dr.execute(inputs=exp.param, final_vars=["load_sin"])
            return result     

        from molexp import tracker

        tracker.mapper = mapper
        # tracker.reducer = resolve(when=ResolveAt.CONFIG_AVAILABLE, decorate_with=lambda: parameterize(**parameters))(reducer)

        dr = (
            driver.Builder()
            .with_modules(tracker)
            .enable_dynamic_execution(allow_experimental_mode=True)
            .with_execution_manager(execution_manager)
            .with_adapter(base.SimplePythonGraphAdapter(base.DictResult()))
            .with_config({settings.ENABLE_POWER_USER_MODE: True})
            .build()
        )
        os.chdir(root)
        results = dr.execute(
                final_vars=[name for name in parameters],
                    # to.pickle(
                    #     id='m_reducer',
                    #     dependencies=['reducer'],
                    #     path='./reduce.pkl'
                    # )
            )
        return results