import pytest
import molexp as me

class TestScript:

    # def test_prettify(self):
    #     script = me.Script('test.txt')
    #     script.content = """
    #     Lorem ipsum dolor sit amet, consectetur adipiscing elit.

    #     Vestibulum condimentum elit vel leo ultricies sodales.

    #     Suspendisse eget erat et nibh aliquam viverra vitae vel ipsum.
    #     Ut convallis felis ac erat posuere, vehicula pellentesque turpis tempor.
    #     Cras volutpat nulla sit amet bibendum interdum.
    #     """

    # def test_append(self):
    #     script = me.Script('test.txt')
    #     script.append("Hello, world!")

    def test_delete(self):
        script = me.Script('test.txt')
        script.content = """
        Lorem ipsum dolor sit amet, consectetur adipiscing elit.

        Vestibulum condimentum elit vel leo ultricies sodales.

        Suspendisse eget erat et nibh aliquam viverra vitae vel ipsum.
        Ut convallis felis ac erat posuere, vehicula pellentesque turpis tempor.
        Cras volutpat nulla sit amet bibendum interdum.
        """
        # script.delete(1)
        
        script.delete((1, 3))

    def test_open(self):
        pass