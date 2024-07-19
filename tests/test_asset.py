import pytest
import shutil
import molexp as me
from pathlib import Path

class TestAsset:

    @pytest.fixture(name='asset', scope='class')
    def test_create(self):

        asset = me.Asset('test', me.Path('test_assets'))

        yield asset

        if asset.work_dir.exists():
            shutil.rmtree(asset._work_dir)

    def test_add_file(self, asset: me.Asset):
        # create dummy file

        with open('test.txt', 'w') as f:
            f.write('test')

        test_path = asset.add_file(me.Path('test.txt'))
        assert test_path == Path(asset.work_dir / 'test.txt')
        assert 'test.txt' in asset.files

        Path('test.txt').unlink()

