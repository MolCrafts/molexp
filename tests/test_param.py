import pytest

import molexp as me

class TestParam:

    @pytest.fixture(name='param', scope='class')
    def test_create(self):

        param = me.Param({
            'test': 'test',
            'test2': 'test2',
            'test3': 'test3',
        })
        return param
    
    