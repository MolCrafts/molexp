import random
import string
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


def random_param(k: int = 3) -> Param:
    """generate a random param.

    Args:
        k (int, optional): lengths of param(dict). Defaults to 3.

    Returns:
        Param: a random param with n key-value paris
    """
    chars = string.ascii_letters
    return Param(
        {
            "".join(random.choices(chars, k=2)): "".join([str(random.randint(0, 99)) for _ in range(2)])
            for _ in range(k)
        }
    )
