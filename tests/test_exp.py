import pytest

import molexp as me
import shutil

class TestExperiment:

    @pytest.fixture(name="exp", scope="class")
    def test_create(self):

        exp = me.Experiment(name="test_experiment", param=me.param.random_param())
        return exp
    
    @pytest.fixture(name="proj", scope="class")
    def test_create_project(self, exp: me.Experiment):

        proj = me.Project(name="test_project")
        proj.add_exp(exp)
        yield proj
        # shutil.rmtree(proj.work_dir)


    def test_def_task(self, exp: me.Experiment):

        exp.def_task(name="test1", param=me.param.random_param())

        exp.def_task(name="test2", param=me.param.random_param())

        exp.def_task(name="test3", param=me.param.random_param())

    def test_list_task(self, exp: me.Experiment):

        task_list = exp.ls()
        assert len(task_list) == 3

    def test_instances(self, proj: me.Project):

        proj.start_task("test_experiment/test1", n_cases=2)

        assert len(list(proj.get_exp("test_experiment").get_tracker(proj.work_dir).work_dir.glob('case*'))) == 2

