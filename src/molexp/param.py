from itertools import product


class Param(dict):

    @property
    def name(self):
        raw = "_".join(f"{k}x{v}" for k, v in self.items())
        return raw.replace(".", "-")

class ParamList(list[Param]):
    pass


class ParamSpace(dict):

    def product(self):
        return ParamList(
            [Param(dict(zip(self.keys(), values))) for values in product(*self.values())]
        )
