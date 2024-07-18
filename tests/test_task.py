import pytest

import molexp as me


class TestTask:

    @pytest.fixture(name='task', scope='class')
    def test_create(self):

        task = me.Task(
            name = 'test_task',
            param = me.param.random_param()
        )
        return task
    
    def test_get_param(self, task: me.Task):

        param = task.get_param()
        assert len(param) == 3

    def test_merge(self):

        # init 3 random tasks
        task1 = me.Task(
            name = 'task1',
            param = me.param.random_param()
        )
        task2 = me.Task(
            name = 'task2',
            param = me.param.random_param()
        )
        task3 = me.Task(
            name = 'task3',
            param = me.param.random_param()
        )

        task = me.Task.union('task', task1, task2, task3)

        assert task.name == 'task'
        assert task.param == task1.param | task2.param | task3.param

    def test_start(self, task):
            
        task.start()

    def test_restart(self, task):

        task.restart()