from pathlib import Path
import shutil

class Asset:

    def __init__(self, name: str, work_dir: Path):
        self.name = name
        self._work_dir = work_dir
        self.metadata = {
            'name': name,
            'files': []
        }

        if not self._work_dir.exists():
            self._work_dir.mkdir(parents=True)
        else:
            # sync all files under folder
            self.sync()

    @property
    def work_dir(self) -> Path:
        return self._work_dir

    @property
    def files(self)->list[str]:
        return self.metadata['files']

    def sync(self):

        for file in self._work_dir.iterdir():
            if file.is_file():
                self.files.append(file.name)

    def add_file(self, file: Path)->Path:
        # copy file to work_dir
        file = file.resolve()
        new_file = self._work_dir / file.name
        shutil.copyfile(file, new_file)
        self.files.append(file.name)
        return new_file
