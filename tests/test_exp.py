import pytest

import molexp as me
import shutil
from tempfile import mkdtemp
from pathlib import Path


class TestExperiment:

    @pytest.fixture(name="exp", scope="class")
    def test_create(self):
        dtemp = Path(mkdtemp())
        exp = me.Experiment(
            name="test_experiment",
            path=dtemp
        )
        return exp

    def test_def_task(self, exp: me.Experiment):

        exp.def_task(name="test1")

        exp.def_task(name="test2")

        exp.def_task(name="test3")

    def test_list_task(self, exp: me.Experiment):

        task_list = exp.ls()
        assert len(task_list) == 3

    def test_create_case(self, exp: me.Experiment):

        n_trials = 5
        exp.init(n_trials=n_trials)
        assert exp.n_trials == n_trials
        assert len(list(filter(lambda x: x.stem.startswith('trial'), exp.path.iterdir()))) == n_trials

    def test_template(self, exp: me.Experiment):
        dtemp = Path(mkdtemp())
        new_exp = exp(name="test_template_exp", path=dtemp)
        assert new_exp.name == "test_template_exp"
