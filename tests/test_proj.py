import pytest
from pathlib import Path

import molexp as me
import business_logic
import os, shutil

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
        exp1.def_task(name="task1", param=me.Param(a='1', c=3.0), modules=[business_logic])

        proj.start_task("exp1/task1")

    # def test_restart_task(self, proj):

    #     proj.restart_task("test1")

    # def test_resume_task(self, proj):
        
    #     proj.resume_task("exp1/task2", me.Param(a='1', c=4.0), modules=[business_logic], config={}, from_task="exp1/task1")

    def test_resume_task_step_by_step(self, proj: me.Project):

        exp1 = proj.get_exp("exp1")
        exp1.resume_task(name='task3', param=me.Param(a='1', c=5.0), modules=[business_logic], config={}, from_files=['task1/*'])

        proj.start_task("exp1/task3")