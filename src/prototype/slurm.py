import inspect
import functools
import subprocess
import os

from pathlib import Path

class Monitor:
    task_queue = []
    def __init__(self, task_id: int, user:str|None):
        self.task_id = task_id
        self.task_queue.append(self)
        self.user = user
    
    def check_status(self):
        if self.user:  # filter result by using user
            check_cmd = f"squeue -u {self.user}"
        check_cmd = f"squeue {self.task_id}"
        subprocess.run(check_cmd, shell=True, capture_output=True, text=True)
        # TODO: parse the output and return status
        # return status

    @classmethod
    def check_all(cls):
        for task in cls.task_queue:
            task.check_status()
        # print/log

    def wait(self):
        while True:
            if self.check_status() == "completed":
                break
            if self == self.task_queue[0]:
                # only "main" monitor can print all tasks' status
                self.check_all()
            # sleep(interval)

def slurm(func, name: str, work_dir: Path | str, slurm_args: dict[str], cmd: list[str]):
    """Decorator to run the result of a function as a command line command."""
    work_dir = Path(work_dir)
    current_dir = Path.cwd()
    slurm_script_name = f"{name}.sub"
    with open(work_dir / slurm_script_name, "w") as f:
        f.write("#!/bin/bash\n")
        for key, value in slurm_args.items():
            f.write(f"#SBATCH {key} {value}\n")
        f.write("\n")
        f.write("\n".join(cmd))
        f.write("\n")

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if inspect.isgeneratorfunction(func):
            # If the function is a generator, we need to block and monitor the task
            gen = func(*args, **kwargs)
            callback_fn = next(gen)
            # Run the command and capture the output
            os.chdir(work_dir) 
            slurm_task_info = subprocess.run(f"sbatch {slurm_script_name}", shell=True, capture_output=True, text=True)
            # slurm_task_info := Submitted batch job 30588834
            slurm_task_id = int(slurm_task_info.stdout.split()[-1])

            # TODO: blocked, but usually many workflow execute in parallel, so we only need a global monitor to pooling all tasks' status 
            monitor = Monitor(slurm_task_id)
            while True:
                if monitor.status == "completed":
                    break
            
            result = callback_fn()
            os.chdir(current_dir)
            try:
                gen.send(result)
                raise ValueError("Generator cannot have multiple yields.")
            except StopIteration as e:
                return e.value
        else:
            # if not a generator, we dont need to monitor it since submit command return no result but job id, only we need to do is capture user manually returned result
            result = func(*args, **kwargs)

            # Run the command and capture the output
            os.chdir(work_dir)  
            slurm_task_info = subprocess.run(f"sbatch {slurm_script_name}", shell=True, capture_output=True, text=True)
            os.chdir(current_dir)

            # NOTE: chdir and change back
            # so I really want a context manager
            # with work_at(work_dir):
            #     do something
            # then back

            # Return the output
            return result

    if inspect.isgeneratorfunction(func):
        # get the return type and set it as the return type of the wrapper
        wrapper.__annotations__["return"] = inspect.signature(func).return_annotation[2]
    return wrapper
