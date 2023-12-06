import pytest
import molexp as me

class TestScript:

    def test_content(self):
        script = me.Script('test')
        script.append('test')
        assert script.content == 'test'

        script.append('test\n asdf')
        assert script.content == 'test\ntest\n asdf'