# author: Roy Kid
# contact: lijichen365@126.com
# date: 2023-12-01
# version: 0.0.1

from pathlib import Path
import string
import textwrap

__all__ = ['Script']

class Script:

    def __init__(self, name:str):
        self.name = name
        self._raw:list[str] = []

    @property
    def text(self)->str:
        return '\n'.join(self._raw)
    
    @text.setter
    def text(self, text:str):
        self._raw = text.split('\n')

    @property
    def content(self)->list[str]:
        return self._raw
    
    @content.setter
    def content(self, text:list[str]):
        self._raw = text

    def append(self, text: str):
        self._raw.extend(text.split('\n'))

    def insert(self, index: int, text: str):
        self._raw[index:index] = text.split('\n')

    def delete(self, index: int | slice):
        if isinstance(index, int):
            del self._raw[index]
        else:
            del self._raw[slice(*index)]

    @classmethod
    def open(self, name):
        with open(name, 'r') as f:
            content = f.readlines()
        script = Script(name)
        script.content = content
        return script

    def save(self, path: Path | str=Path.cwd()):
        path = Path(path)
        text = self.prettify()
        fpath = path / self.name
        if not fpath.exists():
            fpath.parent.mkdir(parents=True, exist_ok=True)
        with open(fpath, 'w') as f:  
            f.write(text)

    def prettify(self)->str:
        text = textwrap.dedent(self.text)
        text = text.strip()
        return text

    def substitute(self, isMissError=False, **kwargs):
        tmp = string.Template(self.text)
        if isMissError:
            text = tmp.substitute(kwargs)
        else:  text = tmp.safe_substitute(kwargs)
        self._raw = text.split('\n')
