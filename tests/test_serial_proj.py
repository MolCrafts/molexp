import pytest
from pathlib import Path
from hamilton.io.materialization import to, from_
from hamilton.function_modifiers import source, save_to, load_from

import molexp as me
import numpy as np

@save_to.pickle(path="./pickle_entry_point.pkl", output_name_="pickle_entry_point")
def entry_point(freq: float, phase: float) -> np.ndarray:
    return np.sin(freq * np.linspace(0, 2 * np.pi, 100) + phase)


def short_task(entry_point: np.ndarray) -> np.ndarray:
    return entry_point


def long_task(freq:float, short_task: np.ndarray) -> np.ndarray:
    return short_task

@load_from.pickle(path="./pickle_long_task.pkl", inject_="sin")
def resume_from_long_task(w: int, theta: int, sin:np.ndarray) -> np.ndarray:
    return np.cos(w * np.linspace(0, 2 * np.pi, 100) + theta) + sin

class TestSerialProj:
    
    @pytest.fixture(name="proj", scope="class")
    def test_init(self):

        proj = me.Project("test_serial_proj", me.Path.cwd(), header={
            "condition1": "default1",
            "condition2": "default2"
        })

        param = me.Param({"freq": 1.0, "phase": 2.0}, alias="test_exp1", description="this is test exp 1")


        import test_serial_proj

        materializers = [
            to.pickle(
                id="pickle_short_task", dependencies=["short_task"], path="./pickle_short_task.pkl"
            ),
            to.pickle(
                id="pickle_long_task", dependencies=["long_task"], path="./pickle_long_task.pkl"
            ),
        ]
        proj.run_exp(param, materializers, test_serial_proj, additional_vars=['pickle_entry_point'])
        yield proj

    def test_list_exp(self, proj: me.Project):
        assert len(proj.ls()) == 1

    def test_resume_from_folder(self, proj: me.Project):

        params = me.Param({"w": 2, "theta": 3}, alias="test_exp2", description="this is test exp 2")
        
        import test_serial_proj

        proj.run_exp(params, [to.pickle(id="pickle_resume_task", dependencies=["resume_from_long_task"], path="./pickle_resume_task.pkl")], test_serial_proj, resume_config={
            'path': proj.query_experiments(lambda exp: exp.run_data['experiment'] == "test_exp1")[0].run_data["run_dir"]
        }, additional_vars=['resume_from_long_task'])

