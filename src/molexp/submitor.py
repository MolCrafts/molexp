from pathlib import Path
import inspect
import functools
import subprocess
import multiprocessing

def setup_adapter(cluster_name: str, cluster_type: str):
    if cluster_type == "slurm":
        return SlurmAdapter(cluster_name)
    else:
        raise ValueError(f"Cluster type {cluster_type} not supported.")
    

class Monitor:
    """
    Global monitor for all jobs submitted. It runs in a separate process and queries the status of all jobs in different clusters.
    """
    monitor_process: subprocess.Popen | None = None
    monitor_jobs: list[int] = []

    def __init__(self, query_mehtod: callable, interval: int = 60):
        self.query_method = query_mehtod
        self.interval = interval

    def start(self):
        if Monitor.monitor_process is None:
            Monitor.monitor_process = multiprocessing.Process(target=self._monitor)
            Monitor.monitor_process.start()

    def watch(self, job_id: int):
        Monitor.monitor_jobs.append(job_id)
        # for example, if we want to submit 20 jobs with a loop, we dont know which submit is last one:
        # ```
        # for inputs in inputs_list:
        #     dr.execute(inputs)
        # ```
        # so we need to restart multiprocessing for each execution
        #   to update monitor_jobs list

        # or we set a global flag e.g.:
        # ```
        # for inputs in inputs_list:
        #     dr.execute(inputs)
        # import Monitor
        # proc: Popen = Monitor.start()
        # ````
        # then we maybe can finish our workflow as non-blocking mode,
        # when we want to know the status of jobs, 
        # we can manually get process from background
        self.start()

    def _monitor(self):
        while True:
            for job_id in self.monitor_jobs:
                info = self.query_method(job_id)
                print(info)


class SubmitAdapter:

    def __init__(self, cluster_name: str, cluster_type: str, is_monitor: bool = True):
        self.cluster_name = cluster_name
        self.cluster_type = cluster_type

        self.queue: list[int] = []
        self.is_monitor = is_monitor
        if self.is_monitor:
            self.monitor = Monitor()

    def __repr__(self):
        return f"<SubmitAdapter: {self.cluster_name}({self.cluster_type})>"
    
    def submit(
        self,
        cmd: list[str],
        job_name: str,
        n_cores: int,
        memory_max: int | None = None,
        run_time_max: str | int | None = None,
        work_dir: Path | str | None = None,
        script_name: str|Path = "run_slurm.sh",
        **args,
    ):
        raise NotImplementedError
    
    def remote_submit(self):
        # TODO: use ssh and scp to submit job to remote cluster
        # third-party library: paramiko
        # license: LGPL
        # https://www.paramiko.org/
        raise NotImplementedError

    def _write_submit_script(self, script_name: str, **args):
        raise NotImplementedError
    
    def query(self, job_id: int|None):
        raise NotImplementedError
    
    def watch(self, job_id: int):
        self.queue.append(job_id)
        if self.is_monitor:
            self.monitor.watch(job_id)



class SlurmAdapter(SubmitAdapter):

    def __init__(self, cluster_name: str):
        super().__init__(cluster_name, "slurm")

    def submit(
        self,
        cmd: list[str],
        job_name: str,
        n_cores: int,
        memory_max: int | None = None,
        run_time_max: str | int | None = None,
        work_dir: Path | str | None = None,
        script_name: str|Path = "run_slurm.sh",
        **slurm_args,
    ):

        slurm_args["--job-name"] = job_name
        slurm_args["--cpus-per-task"] = n_cores
        if memory_max:
            slurm_args["--mem"] = memory_max
        if run_time_max:
            slurm_args["--time"] = run_time_max
        if work_dir:
            slurm_args["--chdir"] = work_dir

        self._write_submit_script(Path(script_name), cmd, **slurm_args)

        proc = subprocess.run(f"sbatch --parsable {script_name}", shell=True, check=True, capture_output=True)
        job_id = int(proc.stdout)

        self.watch(job_id)

        return job_id

    def _write_submit_script(self, script_path: Path, cmd: list[str], **args):
        # assert script_path.exists(), f"Script path {script_path} does not exist."
        with open(script_path, "w") as f:
            f.write("#!/bin/bash\n")
            for key, value in args.items():
                f.write(f"#SBATCH {key} {value}\n")
            f.write("\n")
            f.write("\n".join(cmd))
            f.write("\n")

    def query(self, job_id: int):
        proc = subprocess.run(f"squeue -j {job_id}", shell=True, check=True, capture_output=True)
        info = proc.stdout.split("\n")
        header = info[0]
        info_dict = {k: v for k, v in zip(header.split(), info[1].split())}
        return info_dict

    def remote_submit(self):
        pass


class Submitor:

    cluster: dict[str, SubmitAdapter] = {}

    def __new__(cls, cluster_name: str, cluster_type: str=None):

        if cluster_name not in Submitor.cluster:
            cls.cluster[cluster_name] = setup_adapter(cluster_name, cluster_type)

        return super().__new__(cls)

    def __init__(self, cluster_name: str, *args, **kwargs):
        self._adapter = Submitor.cluster[cluster_name]

    def submit(
        self,
        cmd: list[str],
        job_name: str,
        n_cores: int,
        memory_max: int | None = None,
        run_time_max: str | int | None = None,
        script_name: str|Path|None = "run_slurm.sh",
        uploads: list[Path] | None = None,
        downloads: list[Path] | None = None,
        **slurm_args,
    ):
        job_id = self._adapter.submit(
            cmd, job_name, n_cores, memory_max, run_time_max, script_name, **slurm_args
        )

        return job_id
    
    def query(self, job_id: int):
        return self._adapter.query(job_id)
    
    @property
    def queue(self):
        return self._adapter.queue


def submit(cluster_name: str, cluster_type: str):
    """Decorator to run the result of a function as a command line command."""
    submitor = Submitor(cluster_name, cluster_type)

    def decorator(func):
        if not inspect.isgeneratorfunction(func):
            raise ValueError("Function must be a generator.")

        @functools.wraps(func)
        def wrapper(*args, **kwargs):

            # submit docrated function must be a generator
            generator = func(*args, **kwargs)
            arguments: dict = next(generator)
            is_remote = arguments.pop("is_remote", False)
            if is_remote:
                # submitor = Submitor.remote_submit()
                pass
            else:
                job_id = submitor.submit(**arguments)
            
            try:
                generator.send(job_id)
                # ValueError should not be hit because a StopIteration should be raised, unless
                # there are multiple yields in the generator.
                raise ValueError("Generator cannot have multiple yields.")
            except StopIteration as e:
                result = e.value

            return result

        # get the return type and set it as the return type of the wrapper
        wrapper.__annotations__["return"] = inspect.signature(func).return_annotation
        return wrapper

    return decorator
