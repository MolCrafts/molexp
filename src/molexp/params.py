# author: Roy Kid
# contact: lijichen365@126.com
# date: 2023-12-14
# version: 0.0.1

from typing import Sequence

class Param(dict):

    def __str__(self):
        return '-'.join([f"{k}_{v}" for k, v in self.items()])
    
    def __repr__(self):
        return f"Param({super().__str__()})"
    
    @classmethod
    def from_str(cls, s: str):
        return cls({k: v for k, v in [kv.split('_') for kv in s.split('-')]})

class Params(list):
    
    def __str__(self):
        return f"Params({super().__str__()})"
    
    def __repr__(self):
        return self.__str__()

class ParamSpace(dict):

    def __str__(self):
        return f"ParamSpace({super().__str__()})"
    
    def __repr__(self):
        return self.__str__()

    def product(self, exclude: Sequence[str] = []):
        """
        exclude: list of keys to exclude
        """
        from itertools import product
        keys = sorted([k for k in self.keys() if k not in exclude])
        values = [self[k] for k in keys]
        return Params(
            Param(zip(keys, v))
            for v in product(*values)
        )