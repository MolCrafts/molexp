import os, shutil
from pathlib import Path

import logic
import pytest

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
        if proj.work_dir.exists():
            shutil.rmtree(proj.work_dir)

    def test_def_exp(self, proj: me.Project):

        exp1 = proj.def_exp(name="exp1", param=me.param.random())
        exp1.def_task(name="task1", modules=[logic])
        assert exp1.path == proj.work_dir
        exp2 = proj.def_exp(name="exp2", param=me.param.random())
        assert exp2.path == proj.work_dir
        exp3 = proj.def_exp(name="exp3", param=me.param.random())
        assert exp3.path == proj.work_dir

    def test_list_exp(self, proj):

        exp_list = proj.ls()
        assert len(exp_list) == 3

    # def test_start_all(self, proj: me.Project):

    #     proj.start_all(final_vars=)

    # def test_start_exp(self, proj: me.Project):

    #     exp1 = proj.get_exp("exp1")

    #     exp1.start_exp(final_vars)

    def test_start_task(self, proj: me.Project):

        exp1 = proj.get_exp("exp1")

    # def test_restart_all(self, proj: me.Project):

    #     proj.restart_all()

    # def test_restart_exp(self, proj: me.Project):

    # def test_restart_task(self, proj: me.Project):

    