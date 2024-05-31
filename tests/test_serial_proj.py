import pytest
from pathlib import Path
from hamilton.io.materialization import to, from_
from hamilton.function_modifiers import save_to, source

import molexp as me
import numpy as np


def task1(var1: int, default1: float) -> float:
    return var1 + default1


def task2(task1: float, var2: str) -> dict:
    return {
        "task1": task1,
        "var2": var2,
    }


def task3(var3: str, task2: dict) -> str:
    return task2[var3]

@save_to.file(path=source("exp_dir")/"task4.txt", output_name_="save_task4")
def task4(task3: str) -> str:
    return task3


class TestSerialProj:

    @pytest.fixture(name="proj", scope="class")
    def test_init(self):

        me.Config.USER_NAME = "lijichen365@gmail.com"

        proj = me.Project("test_serial_proj", 1, me.Path.cwd(), header={"default1": 1.23})
        return proj

    def test_run_exp(self, proj: me.Project):
        import test_serial_proj

        param = me.Param({"var1": 1, "var2": "1", "var3": "var2"})
        proj.run_exp(
            "test_serial_proj",
            param,
            test_serial_proj,
            additional_vars=["task3"],
            tags={"test": "test"}
        )

    def test_materialize(self, proj: me.Project):
        import test_serial_proj

        param = me.Param({"var1": 1, "var2": "1", "var3": "var2"})
        proj.run_exp(
            "test_materialize",
            param,
            test_serial_proj,
            materializers=[
                to.pickle(id="pickle_task2", path="./pickle_task2.pkl", dependencies=["task2"]),
            ],
            dag_name="taest_materialize",
            tags={"test": "test", "serialize": "materialize"}
        )

    def test_save_to(self, proj: me.Project):
        
        import test_serial_proj

        param = me.Param({"var1": 1, "var2": "1", "var3": "var2"})
        proj.run_exp(
            "test_materialize",
            param,
            test_serial_proj,
            additional_vars=["save_task4" ],
            dag_name="taest_materialize",
            tags={"test": "test", "serialize": "materialize"}
        )


    # def test_resume_from_folder(self, proj: me.Project):

    #     params = me.Param({"w": 2, "theta": 3}, alias="test_exp2", description="this is test exp 2")

    #     import test_serial_proj

    #     proj.run_exp(
    #         params,
    #         [
    #             to.pickle(
    #                 id="pickle_resume_task",
    #                 dependencies=["resume_from_long_task"],
    #                 path="./pickle_resume_task.pkl",
    #             )
    #         ],
    #         test_serial_proj,
    #         resume_config={
    #             "path": proj.query_experiments(
    #                 lambda exp: exp.run_data["experiment"] == "test_exp1"
    #             )[-1].run_data["run_dir"]
    #         },
    #         additional_vars=["resume_from_long_task"],
    #     )
