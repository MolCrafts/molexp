from itertools import product
from typing import Sequence


class Param(dict):

    def __repr__(self) -> str:
        return f"<Param {super().__repr__()}>"


class ParamList(list[Param]):
    pass


class ParamSpace(dict[str, Sequence]):

    def product(self):
        return ParamList(
            [Param(dict(zip(self.keys(), values))) for values in product(*self.values())]
        )

def random() -> Param:
    import random
    import string
    return Param(
        {key: random.choice(string.ascii_letters) for key in string.ascii_letters}
    )