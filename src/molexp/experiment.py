# author: Roy Kid
# contact: lijichen365@126.com
# date: 2023-12-14
# version: 0.0.1

from pathlib import Path
import random
import logging

class ExperimentGroup(list):
    pass

class Experiment:

    def __init__(self, name:str, root: None | Path | str):
        self.name = name
        self.logger = logging.getLogger(self.name)
        self.root = Path(root)
        self.dir = self.root / self.name
        self.id = hex(random.randint(0, 255))

    def init(self):
        self.dir.mkdir(parents=True, exist_ok=True)
        self.logger.info(f"Experiment {self.name} is created at {self.dir}")

    def exists(self):
        return self.dir.exists()
    
    def load(self):
        self.logger.info(f"Experiment {self.name} is loaded from {self.dir}")
