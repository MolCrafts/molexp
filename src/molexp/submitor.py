import inspect
import functools

from pathlib import Path

from pysqa import QueueAdapter
import time
from hamilton.function_modifiers import tag

print(Path(".queues").absolute())
qa = QueueAdapter(directory=".queues")


class Monitor:
    job_queue = []

    def __init__(self, job_id: int, user: str = None):
        self.job_id = job_id
        self.job_queue.append(self)
        self.user = user

    def get_status(self):
        return qa.get_status_of_job(self.job_id)

    def __del__(self):
        if self in self.job_queue:
            self.job_queue.remove(self)

    def __eq__(self, other: "Monitor"):
        return self.job_id == other.job_id

    def wait(self, pool_interval=5):
        while True:
            status = self.get_status()
            print(status)
            if status == "completed":
                break
            elif status == "failed":
                raise ValueError(f"Job {self.job_id} failed.")
            else:
                print(f"Job {self.job_id} is {status}.")
            time.sleep(pool_interval)

    @classmethod
    def get_all_job_status(cls):
        job_id = [monitor.job_id for monitor in cls.job_queue]
        return qa.get_status_of_jobs(job_id)

    @classmethod
    def wait_all(self, pool_interval=5):
        while True:
            status = self.get_all_job_status()
            if all([s == "completed" for s in status]):
                break
            elif any([s == "failed" for s in status]):
                raise ValueError(f"Job {self.job_id} failed.")
            else:
                print(f"Jobs are {status}.")
            time.sleep(pool_interval)


def submit():
    """Decorator to run the result of a function as a command line command."""

    def decorator(func):
        if not inspect.isgeneratorfunction(func):
            raise ValueError("Function must be a generator.")

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # we don't want to change the current working directory
            # until we are executing the function
            # name = func.__name__

            # If the function is a generator, we need to block and monitor the task if required
            generator = func(*args, **kwargs)
            arguments: dict = next(generator)
            monitor = arguments.pop("monitor", False)
            # Run the command and capture the output
            print(arguments)
            job_id = qa.submit_job(**arguments)
            # slurm_task_info := Submitted batch job 30588834

            # TODO: blocked, but usually many workflow execute in parallel,
            #  so we only need a global monitor to poll all tasks' statuses
            if monitor:
                print(f"monitoring {job_id}")
                # block and monitoring
                monitor = Monitor(job_id)
                monitor.wait()
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
        wrapper.__annotations__["return"] = inspect.signature(
            func
        ).return_annotation  # Jichen: why [2] ?
        return wrapper

    decorator = tag(cmdline="slurm")(decorator)
    return decorator
