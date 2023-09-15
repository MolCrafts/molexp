# author: Roy Kid
# contact: lijichen365@126.com
# date: 2023-09-15
# version: 0.0.1

from datetime import date
from datetime import datetime

from metaflow import FlowSpec, step

class Experiment(FlowSpec):

    def __init__(self, name):
        super().__init__()
        self.name = name
        self.date = date.today()
        self.time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def add_task(self, task):

        task_name = task.name
        setattr(self, task_name, step(task))