from typing import Any
from itertools import product

class Param(dict):

    def __init__(self, params:dict[str, Any]):
        super().__init__(params)

    def copy(self):
        return Param(self)

class ParamList(list[Param]):
    pass


class ParamSpace(dict):

    def product(self):
        return ParamList(
            [Param(dict(zip(self.keys(), values))) for values in product(*self.values())]
        )
