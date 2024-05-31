import molexp as me

class Experiment:

    def __init__(
        self,
        name: str,
        param: me.Param,
        dag_name: str = "1.0.0",
    ):
        self.name = name
        self.param = param
        self.dag_name = dag_name