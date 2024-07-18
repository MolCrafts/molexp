import pytest

import molexp as me

class TestExperiment:

    @pytest.fixture(name="exp", scope="class")
    def test_create(self):

        exp = me.Experiment(name="test_experiment", param=me.param.random_param())
        return exp

    def test_def_task(self, exp: me.Experiment):

        exp.def_task(name="test1", param=me.param.random_param())

        exp.def_task(name="test2", param=me.param.random_param())

        exp.def_task(name="test3", param=me.param.random_param())

    def test_list_task(self, exp: me.Experiment):

        task_list = exp.ls()
        assert len(task_list) == 3
