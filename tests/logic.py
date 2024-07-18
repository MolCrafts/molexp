from pathlib import Path

import numpy as np
from hamilton.function_modifiers import save_to, source, tag


def first_step(a: str, b: int, c: float) -> str:
    return f"first_step: a={a}, b={b}, c={c}"


@save_to.file(path="decorator_save_step.pickle", output_name_="save_by_decorator")
def decorator_save_step(first_step: str, a: str, b: int, c:float) -> str:
    return f"decorator_save_step: a={a}, b={b}, c={c}"


def manual_save_step(first_step: str, a: str, b: int, c:float) -> str:
    with open("manual_save_step.txt", "w") as f:
        f.write(first_step)
    return f"manual_save_step: a={a}, b={b}, c={c}"

call_time = 0

@tag(cache='json')
def workload_step(a: str, b: int, c: float) -> dict:
    global call_time
    call_time += 1
    return {
        'call_time': call_time
    }