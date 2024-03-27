import molexp as me
from pathlib import Path

def print_param(param:me.Param)->me.Param:
    print(param)
    return param

def calc_params(print_param:me.Param)->me.Param:
    print_param['c'] = print_param['a'] + print_param['b']
    return print_param

@me.submit('cluster1', 'slurm')
def submit(calc_params:me.Param)->me.Param:
    
    yield {
        'cmd': [f"echo {calc_params['c']}"],
        'job_name': f"test_{calc_params.name}",
        'n_cores': 1,
        'memory_max': 8,
    }
    return calc_params

class TestProject:

    def test_init_exp(self):

        params = me.ParamSpace({
            'a': [1, 2],
            'b': [3],
            'id': [1, 2]
        })

        param_list = params.product()

        proj = me.Project("test", Path.cwd()/'exp')
        import test_proj
        out = proj.execute(param_list, ['submit'], test_proj)
        print(out)