from itertools import product


class Param(dict):
    
    def name(self):
        return '_'.join(f"{k}x{v}" for k, v in self.items())

class ParamList(list):
    pass

class ParamSpace(dict):
    
    def product(self):
        return ParamList([Param(dict(zip(self.keys(), values))) for values in product(*self.values())])