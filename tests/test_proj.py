from time import sleep
import time
import molexp as me
from pathlib import Path

from hamilton.io.materialization import to
from hamilton.function_modifiers import tag

import numpy as np
import shutil


def entry_point(freq: float, phase: float) -> np.ndarray:
    print(f"Start time: {time.ctime()}")
    return np.sin(freq * np.linspace(0, 2 * np.pi, 100) + phase)


def short_task(entry_point: np.ndarray) -> np.ndarray:
    return entry_point


def long_task(short_task: np.ndarray) -> np.ndarray:
    print("Sleeping for 3 seconds")
    sleep(3)
    return short_task


class TestProject:

    def test_execute_exp(self):

        init_dir = Path.cwd()

        params = me.ParamSpace(
            {
                "freq": [1.0, 2.0, 3.0],
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
        proj.materialize(param_list, materializers, test_proj)

        end_dir = Path.cwd()
        assert init_dir == end_dir

        # if Path('test_exp').exists():
        #     shutil.rmtree('test_exp')

    # def test_list_exp(self):
    #     proj = me.Project("test_exp", Path.cwd())
    #     exp_list = proj.list()
    #     assert len(exp_list) == 3

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
