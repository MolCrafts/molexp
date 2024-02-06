# author: Roy Kid
# contact: lijichen365@126.com
# date: 2023-12-01
# version: 0.0.1

from pathlib import Path
import string
import textwrap

__all__ = ['Script']

class Script:

    def __init__(self, name):
        self.name = name
        self.content:str = ""

    def save(self, path: Path | str = Path.cwd()):
        path = Path(path)
        self.prettify()
        fpath = path / self.name
        if not fpath.exists():
            fpath.parent.mkdir(parents=True, exist_ok=True)
        with open(path / self.name, 'w') as f:  
            f.write(self.content)

    def prettify(self)->str:
        self.content = textwrap.dedent(self.content)

    def substitute(self, isMissError=False, **kwargs):
        tmp = string.Template(self.content)
        if isMissError:
            content = tmp.substitute(kwargs)
        else:  content = tmp.safe_substitute(kwargs)
        self.content = content
