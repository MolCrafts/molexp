import uuid
from typing import Any
from itertools import product

class Param(dict):

    def __init__(self, params:dict[str, Any], alias:str=str(uuid.uuid4()), description:str=""):
        super().__init__(params)
        self.alias = alias
        self.description = description

class ParamList(list[Param]):
    pass


class ParamSpace(dict):

    def product(self):
        return ParamList(
            [Param(dict(zip(self.keys(), values))) for values in product(*self.values())]
        )
