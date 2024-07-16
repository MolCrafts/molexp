from pathlib import Path
import numpy as np

from hamilton.function_modifiers import save_to, source


def first_step(a: str, b: int, c: float) -> str:
    return "first_step"


@save_to.file(path="decorator_save_step.pickle", output_name_="save_by_decorator")
def decorator_save_step(first_step: str) -> str:
    return "decorator_save_step"


def manual_save_step(first_step: str) -> str:
    with open("manual_save_step.txt", "w") as f:
        f.write(first_step)
    return "manual_save_step"
