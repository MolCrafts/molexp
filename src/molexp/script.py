# author: Roy Kid
# contact: lijichen365@126.com
# date: 2023-12-01
# version: 0.0.1

from pathlib import Path
import string
import textwrap

__all__ = ['Script']

class Script:

    def __init__(self, name, ext='txt'):
        self.name = name
        self.ext = ext
        self.full_name = f'{self.name}.{self.ext}'
        self._content_list:list[str] = []

    @property
    def raw(self)->str:
        return self._content_list
    
    @property
    def content(self)->str:
        return '\n'.join(self._content_list)

    def save(self, path: Path | str = Path.cwd()):
        file_name = f'{self.name}.{self.ext}'
        with open(path / file_name, 'w') as f:  
            f.write(self.content)

    def append(self, line: str):
        self._content_list.extend(line.split('\n'))

    def insert(self, line: str, index: int):
        self._content_list.insert(index, line.split('\n'))

    def prettify(self)->str:
        self._content_list = textwrap.dedent(self._content).split('\n')

    def substitute(self, isMissError=False, **kwargs):
        tmp = string.Template(self.content)
        if isMissError:
            content = tmp.substitute(kwargs)
        else:  content = tmp.safe_substitute(kwargs)
        self._content_list = content.split('\n')
