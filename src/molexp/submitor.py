import inspect
import functools
import subprocess
import os

from pathlib import Path

from pysqa import QueueAdapter

print(Path('.queues').absolute())
qa = QueueAdapter(directory=".queues")

class Monitor:
    task_queue = []

    def __init__(self, task_id: int, user: str = None):
        self.task_id = task_id
        self.task_queue.append(self)
        self.user = user

    def check_status(self):
        check_cmd = f"squeue {self.task_id}"
        if self.user:  # filter result by using user
            check_cmd = f"squeue -u {self.user}"
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

    def __del__(self):
        self.task_queue.remove(self)


def submit():
    """Decorator to run the result of a function as a command line command."""


    def decorator(func):
        if not inspect.isgeneratorfunction(func):
            raise ValueError("Function must be a generator.")

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # we don't want to change the current working directory
            # until we are executing the function
            name = func.__name__

            # If the function is a generator, we need to block and monitor the task if required
            generator = func(*args, **kwargs)
            arguments: dict = next(generator)
            monitor = arguments.pop("monitor", False)
            # Run the command and capture the output
            print(arguments)
            job_id = qa.submit_job(
                **arguments
            )
            print(job_id)
            # slurm_task_info := Submitted batch job 30588834

            # TODO: blocked, but usually many workflow execute in parallel,
            #  so we only need a global monitor to poll all tasks' statuses
            if monitor:
                print(f"monitoring {job_id}")
                # block and monitoring
                # _monitor = Monitor(slurm_task_id)
                # while True:
                #     # if _monitor.status == "completed":
                #     #     break
                #     print("monitoring")
                #     break
            try:
                process_result = {"job_id": job_id}
                generator.send(process_result)
                # ValueError should not be hit because a StopIteration should be raised, unless
                # there are multiple yields in the generator.
                raise ValueError("Generator cannot have multiple yields.")
            except StopIteration as e:
                result = e.value

            # change back to the original working directory
            # Return the output
            return result

        # get the return type and set it as the return type of the wrapper
        print(wrapper.__annotations__)
        print(inspect.signature(func).return_annotation)
        wrapper.__annotations__["return"] = inspect.signature(func).return_annotation  # Jichen: why [2] ?
        return wrapper

    return decorator
