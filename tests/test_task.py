import pytest

import molexp as me
from tempfile import mkdtemp
from pathlib import Path

class TestTask:

    @pytest.fixture(name='task', scope='class')
    def test_create(self):
        dir = Path(mkdtemp())
        task = me.Task(
            name = 'test_task',
            path = dir,
            param = me.Param.random(3),
        )
        return task
    
    def test_get_param(self, task: me.Task):

        param = task.get_param()
        assert len(param) == 3

    def test_merge(self):
        dir = Path(mkdtemp())
        # init 3 random tasks
        task1 = me.Task(
            name = 'task1',
            path = dir
        )
        task2 = me.Task(
            name = 'task2',
            path = dir
        )
        task3 = me.Task(
            name = 'task3',
            path = dir
        )

        task = me.Task.union('task', task1, task2, task3)

        assert task.name == 'task'
        assert task.param == task1.param | task2.param | task3.param
