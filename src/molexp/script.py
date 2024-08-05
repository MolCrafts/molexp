import string
from pathlib import Path

class Script:

    def __init__(self, name:str):
        self.name = name

    def __str__(self) -> str:
        return self.content
    
    def substatute(self, **kwargs) -> None:
        tmp = string.Template(self.content)
        self.content = tmp.safe_substitute(kwargs)
    
    def save(self, path:str|Path) -> None:
        with open(path, "w") as f:
            f.write(self.content)

    def load(self, path:str|Path) -> None:
        with open(path, "r") as f:
            self.content = f.read()
