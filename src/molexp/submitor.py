# author: Roy Kid
# contact: lijichen365@126.com
# date: 2023-12-01
# version: 0.0.1

from .script import Script
from pathlib import Path
import subprocess
import logging
from functools import wraps


__all__ = ['SlurmSubmitor', 'submit', 'work_at']

class Monitor:

    def __init__(self):

        self.job_pool = {}
        self.logger = logging.getLogger(self.__class__.__name__)

    def add_job(self, job_id, job_name):
        self.job_pool[job_id] = job_name

    def remove_job(self, job_id):
        self.job_pool.pop(job_id)

    def status(self, job_id):
        out = subprocess.check_output(f'squeue -j {job_id}', shell=True)
        out = out.decode('utf-8')
        lines = out.split('\n')
        line = list(map(lambda x: x.startswirh(str(job_id)), lines))
        assert len(line) > 0, 'multiple jobs found, please check!'
        # 29417953 tetralith tg_mil1_  x_jicli  R      23:51      2 n[1790,1795]
        job_id, partition, name, user, status, time, nodes, node_info = line[0].split(' ')
        self.logger.info(f'Job {job_id} status: {status}')
        return status
        

class SlurmSubmitor(Script):

    monitor = Monitor()

    def __init__(self, config:dict, name:str='submit', ext:str='sh', is_block:bool=False):
        super().__init__(name, ext)
        self._config = config
        self.logger = logging.getLogger(self.__class__.__name__)

    def submit(self, path: Path | str = Path.cwd()):
        # write down
        config_lines = []
        config_lines.append('#!/bin/bash')
        for k, v in self._config.items():
            config_lines.append(f'#SBATCH {k} {v}')
        config_lines.append('\n')

        self._content_list = config_lines + self._content_list

        path = Path(path)
        self.save(path)

        proc = subprocess.call(f'sbatch {self.full_name}', shell=True, cwd=path, capture_output=True)
        stdout = proc.stdout.decode('utf-8')
        stderr = proc.stderr.decode('utf-8')
        if stderr:
            self.logger.error(f'Submit {self.full_name} to {path} failed!')
            self.logger.error(stderr)
        else:
            # example: Submitted batch job 123456
            job_id = stdout.split(' ')[-1]
            self.logger.info(f'{self.full_name}({job_id}) submitted')
        
        if self.is_block:
            self.monitor.add_job(job_id, self.full_name)
            while self.monitor(job_id) == 'R':
                time.sleep(60)

def work_at(workdir:Path|str=Path.cwd()):
    def _cd(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            olddir = os.cwd()
            os.chdir(workdir)
            results = func(*args, **kwargs)
            os.chdir(olddir)
            return results
        return wrapper
    return _cd

def submit(cmd:str, type:str, is_block:bool, config:dict, name:str='submit', ext:str='sh'):
    def _submit(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            if type == 'slurm':
                submitor = SlurmSubmitor(config, name, ext, is_block)
                submitor.append(cmd)
                submitor.submit(os.cwd())
            else:
                raise NotImplementedError(f'submit type {type} not implemented!')
            return result
        return wrapper
    return _submit