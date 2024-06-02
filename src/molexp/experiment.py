from pathlib import Path
import datetime
from dataclasses import dataclass, asdict
from typing import Any, Dict
import hashlib
import inspect
import json
import logging
import os

from copy import deepcopy
from hamilton import graph_types, lifecycle

import molexp as me

logger = logging.getLogger(__name__)


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
        self.tasks = []

    def get_param(self):
        return self.param.copy()
    
    def get_config(self):
        return dict(self.config)
    
    def get_tracker(self, work_dir: str | Path = Path.cwd()):
        return ExperimentTracker(
            self.name,
            self.param,
            self.config,
            work_dir,
        )

class Task:

    def __init__(
        self,
        name: str,
        param: me.Param,
        config: dict = {},
    ):
        self.name = name
        self.param = param
        self.config = config

    def get_param(self):
        return self.param.copy()
    
    def get_config(self):
        return dict(self.config)
        
    def get_tracker(self, work_dir: str | Path = Path.cwd()):
        return TaskTracker(
            self.name,
            self.param,
            self.config,
            work_dir,
        )

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
        self, *args, node_tags: dict, **kwargs
    ):
        os.chdir(self.work_dir)

    def run_after_node_execution(self, *, node_name: str, node_tags: Dict[str, Any], node_kwargs: Dict[str, Any], node_return_type: type, result: Any, error: Exception | None, success: bool, task_id: str | None, run_id: str, **future_kwargs: Any):
        os.chdir(self.init_dir)


class ExperimentTracker(
    # lifecycle.NodeExecutionHook,
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
