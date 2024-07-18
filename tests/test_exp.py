import pytest

import molexp as me
import logic

class TestExperiment:

    @pytest.fixture(name="exp", scope="class")
    def test_create(self):

        exp = me.Experiment(name="test_experiment", param=me.param.random_param())
        return exp

    def test_def_task(self, exp):

        exp.def_task(name="test1", param=me.param.random_param(), modules=[logic])

        exp.def_task(name="test2", param=me.param.random_param(), modules=[logic])

        exp.def_task(name="test3", param=me.param.random_param(), modules=[logic])

    def test_list_task(self, exp):

        task_list = exp.ls()
        assert len(task_list) == 3

    # def test_restart(self, exp):

    #     exp.restart(name="test1", param=me.Param({}))

    # def test_resume(self, exp):

    #     exp.resume(name="test1", param=me.Param({}))
