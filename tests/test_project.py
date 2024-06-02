import pytest
from pathlib import Path
from hamilton.io.materialization import to, from_
from hamilton.function_modifiers import save_to, source

import molexp as me
import numpy as np


class TestSerialProj:

    @pytest.fixture(name="proj", scope="class")
    def init_proj(self):

        me.Config.USER_NAME = "lijichen365@gmail.com"

        proj = me.Project("test_serial_proj", Path.cwd())
        return proj

    @pytest.fixture(name="task", scope="class")
    def init_param(self):

        root = Path.cwd()

        return me.Task(
            "test_task",
            me.Param(
                {
                    "task_id": 1,
                    "exp_id": 1,
                    "task_value": "task_value",
                }
            ),
            config={},
        )

    @pytest.fixture(name="exp", scope="class")
    def init_exp(self):

        root = Path.cwd()

        return me.Experiment(
            "test_exp",
            me.Param(
                {
                    "exp_id": 2,
                    "exp_value": "exp_value",
                }
            ),
            config={},
        )

    def test_run_exp(self, proj: me.Project, exp: me.Experiment, task: me.Task):

        import business_logic

        proj.run(task, exp, business_logic)


class TestParallelProj:

    @pytest.fixture(name="proj", scope="class")
    def init_proj(self):

        me.Config.USER_NAME = "lijichen365@gmail.com"

        proj = me.Project("test_parallel_proj", Path.cwd())
        return proj

    @pytest.fixture(name="tasks", scope="class")
    def init_tasks(self):

        param_space = me.ParamSpace({"task_id": [1, 2, 3], "task_value": ["task_value"]})

        params = param_space.product()
        for i, param in enumerate(params):
            param["task_dir"] = Path.cwd() / "test_parallel_proj" / "test_exp" / f"test_task_{i}"
            param["proj_dir"] = Path.cwd() / "test_parallel_proj"

        tasks = [me.Task(f"test_task_{i}", param, config={}) for i, param in enumerate(params)]

        return tasks

    @pytest.fixture(name="exp", scope="class")
    def init_exp(self):

        return me.Experiment(
            "test_exp",
            me.Param(
                {
                    "exp_id": 1,
                    "exp_value": "exp_value",
                }
            ),
            config={},
        )

    def test_run_exp(self, proj: me.Project, exp: me.Experiment, tasks: list[me.Task]):

        import business_logic

        proj.batch_run(tasks, [exp], business_logic)
