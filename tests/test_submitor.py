import pytest
import molexp as me

from molexp.submitor import SlurmAdapter
import subprocess
from pathlib import Path

import tempfile

class MockSlurm:

    class CompletedProcess:
        pass

    @staticmethod
    def query():
        cp = MockSlurm.CompletedProcess()
        cp.stdout = """             JOBID PARTITION     NAME     USER ST       TIME  NODES NODELIST(REASON)
           3355077      long  NMNMPx8 jichenli  R 2-01:35:40      1 nid002629"""
        return cp
    
    @staticmethod
    def job_id():
        cp = MockSlurm.CompletedProcess()
        cp.stdout = "3355077"
        return cp


@pytest.fixture
def mock_slurm(monkeypatch):

    def run(*args, **kwargs):
        cmd = args[0].split()
        if "squeue" in cmd:
            return MockSlurm.query()
        elif "sbatch" in cmd:
            if "--parsable" in cmd:
                return MockSlurm.job_id()
        
    monkeypatch.setattr(subprocess, "run", run)


class TestSlurmSubmitor:
    
    def test_write_submit_script(self):
        tmp = tempfile.NamedTemporaryFile(delete=False)
        submitor = SlurmAdapter("test")
        submitor._write_submit_script(tmp.name, cmd=["echo hello", "ls -al"], **{"--job-name": "test_job", "--cpus-per-task": 1})

        with open(tmp.name, "r") as f:
            lines = f.readlines()
            assert lines[0].strip() == "#!/bin/bash"
            assert lines[1].strip() == "#SBATCH --job-name test_job"
            assert lines[2].strip() == "#SBATCH --cpus-per-task 1"
            assert lines[3].strip() == ""
            assert lines[4].strip() == "echo hello"
            assert lines[5].strip() == "ls -al"

    def test_submit(self, mock_slurm):
        tmp = tempfile.NamedTemporaryFile()
        submitor = SlurmAdapter("test")
        job_id = submitor.submit(["echo hello", "ls -al"], job_name="test_job", n_cores=1, script_name=tmp.name)
        assert job_id in submitor.queue

    def test_query(self, mock_slurm):
        submitor = SlurmAdapter("test")
        job_id = 3355077
        info = submitor.query(job_id)
        assert info["JOBID"] == "3355077"
        assert info["PARTITION"] == "long"
        assert info["NAME"] == "NMNMPx8"
        assert info["USER"] == "jichenli"
        assert info["ST"] == "R"
        assert info["TIME"] == "2-01:35:40"
        assert info["NODES"] == "1"
        assert info["NODELIST(REASON)"] == "nid002629"


class TestSubmitor:

    def test_submit(self):
        pass

class TestSubmitDecorator:

    def test_submit(self, mock_slurm):
        
        tmp = tempfile.NamedTemporaryFile(delete=False)
        @me.submit("test", "slurm")
        def test_func()->int:
            job_id = yield {
                "cmd": ["echo hello", "ls -al"],
                "job_name": "test_job",
                "n_cores": 1,
                "script_name": tmp.name
            }
            assert job_id in me.Submitor("test").queue
            return job_id
        
        test_func()
