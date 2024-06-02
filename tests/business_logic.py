from pathlib import Path
import numpy as np

from hamilton.function_modifiers import save_to, source


def internal_save_stage(task_id: int, exp_id: int, task_value:str) -> int:

    with open("internal_save.txt", "w") as f:
        f.write(str(task_value))

    return task_id, 


@save_to.file(path="decorator_save.pickle", output_name_="save_decorator")
def decorator_save_stage(task_id: int, exp_id: int, task_value:str) -> str:
    return str(task_value)
