import pytest

import molexp as me
import shutil
import tempfile


class TestExperiment:

    @pytest.fixture(name="exp", scope="class")
    def test_create(self):

        exp = me.Experiment(
            name="test_experiment",
            path=tempfile.TemporaryDirectory(),
            param=me.param.random(),
        )
        return exp

    def test_def_task(self, exp: me.Experiment):

        exp.def_task(name="test1", param=me.param.random())

        exp.def_task(name="test2", param=me.param.random())

        exp.def_task(name="test3", param=me.param.random())

    def test_list_task(self, exp: me.Experiment):

        task_list = exp.ls()
        assert len(task_list) == 3

    def test_create_case(self, exp: me.Experiment):

        exp.init(n_trials=10)
        assert len(exp.n_trials) == 10
        assert len(list(exp.path.iterdir())) == 10

    def test_template(self, exp: me.Experiment):

        new_exp = exp(name="test_template_exp", path=tempfile.TemporaryDirectory())
        assert new_exp.name == "test_template_exp"
        assert new_exp.n_trials == exp.n_trials
