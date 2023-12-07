# author: Roy Kid
# contact: lijichen365@126.com
# date: 2023-11-27
# version: 0.0.1

import shutil
from typing import Literal
import hamilton
from hamilton import base, driver
from hamilton.execution import executors
from pathlib import Path
from time import ctime
import inspect
import textwrap
import importlib

__all__ = ["Experiment"]


def convert_path_to_module(path):
    module_path = Path(path).resolve()
    module_name = module_path.relative_to(Path.cwd())
    module = module_name.stem
    package = module_name.parent.as_posix().replace("/", ".")
    return "." + module, package


class Experiment:
    def __init__(self, name, description, root: None | Path | str = None):
        self.name = name
        self.description = description
        self.root = Path(root or Path.cwd())
        print(self.root)
        self.init_workdir()

        self.tasks = []
        self.dag_paths = []

    def init_workdir(self):
        workdir = self.root / f"exp_{self.name}"
        workdir.mkdir(exist_ok=True)
        self.work_dir = workdir
        # write metadata
        with open(workdir / f"{self.name}.log", "w") as f:
            f.write(f"# exp: {self.name}\n")
            f.write(f"- time: {ctime()}\n")
            f.write(f"## description:\n")
            f.write(f"{self.description}\n")
            f.write(' --- \n')

    def add_task(self, callable):
        self.tasks.append(callable)

    def add_tasks(self, module_path: Path | str):
        path = Path(module_path)
        shutil.copy(path, self.work_dir)
        self.dag_paths.append(self.work_dir / path.name)

    def export_dag(self, dag_name:str = "dag.py"):
        src = ""
        dag_path = self.work_dir / dag_name
        with open(dag_path, "w") as f:
            for node in self.tasks:
                src += inspect.getsource(node)
            wrapped_src = textwrap.dedent(
                src,
            )
            f.write(wrapped_src)
        return dag_path

    def execute(self, input:dict, output:list, config={}, adapter=None):
        dag_module = []
        if self.tasks:
            dag_path = self.export_dag()
            self.dag_paths.append(dag_path)

        for path in self.dag_paths:
            module, package = convert_path_to_module(path)
            dag_module.append(importlib.import_module(module, package))
        # adapter = base.SimplePythonGraphAdapter()

        dr = (
            driver.Builder()
            .with_modules(*dag_module)
            .enable_dynamic_execution(allow_experimental_mode=True)
            .with_config({})
            .with_local_executor(executors.SynchronousLocalTaskExecutor())
            .with_remote_executor(executors.MultiProcessingExecutor(max_tasks=5))
            .build()
        )
        out = dr.execute(output, inputs=input)
        return out

if __name__ == "__main__":
    exp = Experiment("test", "test experiment")

    def foo(a: int, b: float) -> float:
        """
        add

        Args:
            a (int): input
            b (float): input

        Returns:
            float: outputput
        """
        return a + b

    exp.add_task(foo)
    out = exp.execute(input={"a": 1, "b": 2.0}, output=["foo"])