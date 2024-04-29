import molexp as me
from pathlib import Path

from hamilton.io.materialization import to
from hamilton.function_modifiers import tag

import numpy as np

def calc_theta(freq:float, phase:float)->np.ndarray:
    theta = np.linspace(0, 2*np.pi, 100)
    return freq * theta + phase

def calc_sine(calc_theta:np.ndarray)->np.ndarray:
    sin = np.sin(calc_theta)
    return sin

def load_sin(freq:float, phase:float)->np.ndarray:
    sin = np.load('sine.pkl', allow_pickle=True)
    return sin

class TestProject:

    def test_init_exp(self):

        if Path('test_exp').exists():
            import shutil
            shutil.rmtree('test_exp')
        
        init_dir = Path.cwd()

        params = me.ParamSpace({
            'freq': [1., 2., 3.],
            'phase': [0.],
        })

        param_list = params.product()

        proj = me.Project("test_exp", Path.cwd())
        import test_proj
        materializers = [
            to.pickle(
                id='m_calc_sine',
                dependencies=['calc_sine'],
                path='./sine.pkl'
            )
        ]
        proj.execute(param_list, materializers, test_proj)
        
        end_dir = Path.cwd()
        assert init_dir == end_dir

    def test_list_exp(self):
        proj = me.Project("test_exp", Path.cwd())
        exp_list = proj.list()
        assert len(exp_list) == 3

    def test_map_reduce(self):
        proj = me.Project("test_exp", Path.cwd())
        exp_group = proj.list()
        import test_proj

        mp = [
            to.pickle(
                id='m_load_sin',
                dependencies=['load_sin'],
                path='./sine1.pkl'
            )
        ]
        def reducer(mapper:dict)->np.ndarray:
            results = {}
            for exp_name, mat_result in mapper.items():
                results[exp_name] = np.load(mat_result[0]['m_load_sin']['file_metadata']['path'], allow_pickle=True)
            # calculate mean
            return np.mean([result for result in results.values()], axis=0)
        
        results = exp_group.map_reduce(mp, reducer, test_proj)
        print(results)
        assert False