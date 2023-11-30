# author: Roy Kid
# contact: lijichen365@126.com
# date: 2023-11-27
# version: 0.0.1

import hamilton
from hamilton import base, driver
from hamilton.execution import executors
from pathlib import Path
from time import ctime
import inspect
import textwrap
import importlib


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
        self.root = root or Path.cwd()
        self.init_workdir()

        self.tasks = []

    def init_workdir(self):
        workdir = self.root / f"exp_{self.name}"
        workdir.mkdir(exist_ok=True)
        self.work_dir = workdir
        # write metadata
        with open(workdir / "readme.md", "w") as f:
            f.write(f"# exp: {self.name}\n")
            f.write(f"- time: {ctime()}\n")
            f.write(f"## description:\n")
            f.write(f"{self.description}\n")

    def add_task(self, callable):
        self.tasks.append(callable)

    def export_dag(self):
        src = ""
        dag_path = self.work_dir / "dag.py"
        with open(dag_path, "w") as f:
            for node in self.tasks:
                src += inspect.getsource(node)
            wrapped_src = textwrap.dedent(
                src,
            )
            print(src)
            f.write(wrapped_src)
        return dag_path

    def execute(self, input, output, config={}, adapter=None):
        dag_path = self.export_dag()
        module, package = convert_path_to_module(dag_path)
        dag_module = importlib.import_module(module, package)

        # adapter = base.SimplePythonGraphAdapter()

        dr = (
            driver.Builder()
            .with_modules(dag_module)
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
    print(out)
