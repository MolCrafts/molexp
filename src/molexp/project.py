from functools import partial
from hamilton import driver
from hamilton.experimental.h_cache import CachingGraphAdapter

from hamilton.plugins import h_experiments
from hamilton.function_modifiers import value, parameterize, resolve, ResolveAt
from hamilton.execution.executors import DefaultExecutionManager
from hamilton.execution import executors

import os
import json
from pathlib import Path

from .param import Param, ParamList
from hamilton import settings

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


class Project:

    def __init__(self, name: str, work_dir: str | Path = Path.cwd()):

        self.name = name
        self._root = Path(work_dir).absolute() / name

    @property
    def root(self) -> Path:
        return self._root

    def materialize(self, param_list: ParamList, materializers: list, *modules: list):

        parameters = {param.name: {"param": value(param)} for param in param_list}

        @resolve(when=ResolveAt.CONFIG_AVAILABLE, decorate_with=lambda: parameterize(**parameters))
        def materialize_exp(param: Param) -> str:
            tracker = h_experiments.ExperimentTracker(param.name, self.root)
            dr = (
                driver.Builder()
                .with_modules(*modules)
                .enable_dynamic_execution(allow_experimental_mode=True)
                .with_adapters(tracker)
                .build()
            )
            dr.materialize(*materializers, inputs=param)
            return tracker.run_id

        from molexp import project
        project.materialize_exp = materialize_exp

        execution_manager = DefaultExecutionManager(
            executors.MultiProcessingExecutor(8),
            executors.MultiProcessingExecutor(8),
        )

        dr = (
            driver.Builder()
            .with_modules(project)
            .enable_dynamic_execution(allow_experimental_mode=True)
            .with_execution_manager(execution_manager)
            .with_config({settings.ENABLE_POWER_USER_MODE: True})
            .build()
        )

        dr.execute(
            final_vars=[name for name in parameters],
        )
