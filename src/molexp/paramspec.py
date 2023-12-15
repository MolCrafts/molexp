# author: Roy Kid
# contact: lijichen365@126.com
# date: 2023-12-14
# version: 0.0.1

from typing import Sequence

class ParamSpec(dict):

    def product(self, exclude:Sequence[str]=[]):
        from itertools import product
        keys = [k for k in self.keys() if k not in exclude]
        values = [self[k] for k in keys]