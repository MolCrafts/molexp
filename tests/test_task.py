import pytest

import molexp as me

class TestTask:

    @pytest.fixture(name='task', scope='class')
    def test_create(self):

        task = me.Task(
            name = 'test_task',
            param = me.param.random_param(),
        )
        return task
    
    def test_get_param(self, task: me.Task):

        param = task.get_param()
        assert len(param) == 3

    def test_start(self, task):
            
        task.start()

    def test_restart(self, task):

        task.restart()