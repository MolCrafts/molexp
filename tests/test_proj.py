import os, shutil
from pathlib import Path

import logic
import pytest
from pytest_mock import MockerFixture

import molexp as me


class TestProject:

    @pytest.fixture(name="proj", scope="class")
    def test_create(self):

        proj = me.Project(
            name="test_project",
            description="This is a test project",
            tags=["test", "project"],
        )
        yield proj

    @pytest.fixture(scope='class', autouse=True)
    def delete_temp(self):
        init_dir = Path.cwd()
        yield
        os.chdir(init_dir)
        proj_path = Path('test_project')
        assert proj_path.absolute().exists()
        if proj_path.exists():
            shutil.rmtree(proj_path)

    def test_def_exp(self, proj: me.Project):

        proj.def_exp(name="exp1", param=me.Param(a='1', b=1))

        proj.def_exp(name="exp2", param=me.Param(a='2', b=2))

        proj.def_exp(name="exp3", param=me.Param(a='3', b=3))

    def test_list_exp(self, proj):

        exp_list = proj.ls()
        assert len(exp_list) == 3

    # def test_start_all(self, proj):

    #     proj.start_all()

    def test_start_task(self, proj:me.Project):

        exp1 = proj.get_exp("exp1")
        exp1.def_task(name="task1", param=me.Param(a='1', c=3.0), modules=[logic])

        proj.start_task("exp1/task1", final_vars=['manual_save_step'])

    def test_caching(self, proj: me.Project, mocker: MockerFixture):

        exp1 = proj.get_exp("exp1")
        exp1.def_task(name='task2', param=me.Param(a='1', c=4.0), modules=[logic])

        result = proj.start_task("exp1/task2", final_vars=['workload_step'])
        print(result)
        assert result['workload_step']['call_time'] == 1
        result = proj.start_task("exp1/task2", final_vars=['workload_step'])
        assert result['workload_step']['call_time'] == 1
        
        # test recompute
        # task = exp1.get_task('task2')
        # task.param['a'] = '2'
        # result = proj.start_task("exp1/task2", final_vars=['workload_step'])
        # assert result['workload_step']['call_time'] == 2