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

        exp1 = proj.def_exp(name="exp1")
        exp1.def_task(name="task1", modules=[logic])
        assert exp1.path == proj.work_dir
        exp2 = proj.def_exp(name="exp2")
        assert exp2.path == proj.work_dir
        exp3 = proj.def_exp(name="exp3")
        assert exp3.path == proj.work_dir
        assert len(proj.ls()) == 3

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
        task1 = exp1.get_task("task1")
        proj.start_task(task1, final_vars={"a": 1, "b": 2})

    def test_start_tasks(self, proj: me.Project):

        exp2 = proj.get_exp("exp2")
        task1 = exp2.def_task("task1", modules=[logic])
        task2 = exp2.def_task("task2", modules=[logic])
        task3 = exp2.def_task("task3", modules=[logic])
        task4 = exp2.def_task("task4", modules=[logic])

        proj.start_tasks(
            [task1, task2, task3, task4],
            params=[{"a": "1", "b": 2, "c": 3.0}, {"a": "2", "b": 3, "c": 4.0}, {"a": "3", "b": 4, "c": 5.0}, {"a": "4", "b": 5, "c": 6.0}],
            final_vars=["sleep_report_step"],
        )

    # def test_restart_all(self, proj: me.Project):

    #     proj.restart_all()

    # def test_restart_exp(self, proj: me.Project):

    # def test_restart_task(self, proj: me.Project):
