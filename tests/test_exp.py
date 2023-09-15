# author: Roy Kid
# contact: lijichen365@126.com
# date: 2023-09-15
# version: 0.0.1

import pytest

from molexp.experiment import Experiment
from molexp.task import Task

class TestExp:

    def test_init(self):

        exp = Experiment('init')
        assert exp.name == 'init'

    def test_add_task(self):

        exp = Experiment('add_task')
        task = Task('task')
        exp.add_task(task)
        