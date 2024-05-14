import molexp as me
from pathlib import Path

from hamilton.io.materialization import to, from_

import numpy as np
import pytest


def entry_point(freq: float, phase: float) -> np.ndarray:
    return np.sin(freq * np.linspace(0, 2 * np.pi, 100) + phase)


def short_task(entry_point: np.ndarray) -> np.ndarray:
    return entry_point


def long_task(freq:float, short_task: np.ndarray) -> np.ndarray:
    return short_task


def resume_from_long_task(resume_from_long_task:np.ndarray) -> np.ndarray:
    return resume_from_long_task
    

class TestProject:

    @pytest.fixture(name="proj", scope="class")
    def test_init_exp(self):

        params = me.ParamSpace(
            {
                "freq": [1.0],
                "phase": [0.0],
            }
        )

        param_list = params.product()

        proj = me.Project("test_exp", Path.cwd())
        import test_proj

        materializers = [
            to.pickle(
                id="pickle_short_task", dependencies=["short_task"], path="./pickle_short_task.pkl"
            ),
            to.pickle(
                id="pickle_long_task", dependencies=["long_task"], path="./pickle_long_task.pkl"
            ),
        ]
        proj.run_exps(param_list, materializers, test_proj)
        yield proj

    def test_list_exp(self, proj: me.Project):
        assert len(proj.list()) == 1

    def test_resume_latest(self, proj: me.Project):

        proj = me.Project("test_exp", Path.cwd())
        params = me.ParamSpace(
            {
                "freq": [1.0],
                "phase": [0.0],
            }
        )

        materializers = [
            from_.pickle(
                path="./pickle_long_task.pkl",
                target="resume_from_long_task", 
            ),
            to.pickle(
                id="pickle_resume_from_long_task", dependencies=["resume_from_long_task"], path="./pickle_resume_from_long_task.pkl"
            )
        ]
        import test_proj
        param_list = params.product()
        proj.run_exps(param_list, materializers, test_proj, resume=True)
        

    # def test_map_reduce(self):
    #     proj = me.Project("test_exp", Path.cwd())
    #     exp_group = proj.list()
    #     import test_proj

    #     mp = [
    #         to.pickle(
    #             id='m_load_sin',
    #             dependencies=['load_sin'],
    #             path='./sine1.pkl'
    #         )
    #     ]
    #     def reducer(mapper:dict)->np.ndarray:
    #         print(mapper)
    #         results = {}
    #         for exp_name, mat_result in mapper.items():
    #             results[exp_name] = np.load(mat_result[0]['m_load_sin']['file_metadata']['path'], allow_pickle=True)
    #         # calculate mean
    #         return np.mean([result for result in results.values()], axis=0)

    #     results = exp_group.map_reduce(mp, reducer, test_proj)
    #     print(results)
    #     assert False
