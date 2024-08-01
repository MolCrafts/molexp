import pytest

import molexp as me
import numpy as np

class TestParam:

    @pytest.fixture(name='param', scope='class')
    def test_create(self):

        param = me.Param({
            'str': 'test',
            'int': 42,
            'float': 3.14,
            'bool': True,
            'list': [1, 2, 3],
            'numpy': np.array([1, 2, 3]),
        })
        return param
    
    def test_serialize(self, param):
        import pickle, shutil
        s = pickle.dumps(param)
        p = pickle.loads(s)
        assert param == p
        
    
    
    