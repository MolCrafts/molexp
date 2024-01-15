# an draft for Hamilton orchestrating slurm

A workflow about how to setup a general molecular dynamics simulation is demostrated. 

Typically, three type of tasks are involved during this process:
- fast(python/cmdlines in sec/min) and resource-free: do not need to submit to slurm, can be done locally
- slow(hours/days) and little resource: need to submit to slurm, but can be done by using little resource and short time
- slow and large resource(days-months): need to submit to slurm, and need to use large resource and long time

As for monitoring, slow and little resource tasks need to be monitored more because it is usually in the middle of the graph and you need to wait until it completes before starting the next task. Long time running tasks may not need to be monitored because we dont care it intermediate state, but it is still need to check it(can be done manually, but better to cache task ids when submite them).

Two types of monitoring can be used: monitoring if files are generated or some specific lines are printed in the log file; monitoring if the task is still running. When a requirement is met, next action would be triggered.

```
preprocessing - long-time running for sampling - postprcessing
                    | result1     | result2            |
                  node(result1)  node(result2)
```

To submit a task to slurm, a slurm script(bash script) is needed. The template looks like this:

``` bash
#!/bin/bash
#SBATCH -A snic2022-5-658  
#SBATCH -n 64
#SBATCH -J jobname
#SBATCH -o out
#SBATCH -t 07-00:00:00

...bash command or cmdlines
```

Since here is tons of configuration, it is better to generate `#SBATCH` lines by a flexible dict. After a script(`submit.sh`) is generated, `sbatch submit.sh` in termial will submit the task to slurm.

To retrive the status, `squeue -u $USER` should be used, and will print following content:
```
JOBID PARTITION                 NAME     USER ST       TIME  NODES NODELIST(REASON)
29993320 tetralith                  peo  x_jicli  R       2:53      2 n[1125,1268]
29993321 tetralith                  peo  x_jicli  R       2:53      2 n[1269,1271]
29993322 tetralith                  peo  x_jicli  R       2:53      2 n[1272-1273]
```

`NAME` is the job name, specified by `#SBATCH -J jobname`. `ST` is the status, `R` means running, `PD` means pending, etc. 

## Example

Here I provide a three-stages example, which is a typical molecular dynamics simulation:
1. prepare: prepare the initial structure and topology, it is a resource-free task, and can be done locally
2. equilibrium: run a long-time simulation to equilibrate the system, it is a slow and little resource task, may takes days
3. production: by using the equilibrated structure which generated by stage 2, to calculate the properties

In the `run.py`, I want to write the molexp(wrapper for hamilton because scientific experiment/simulation has some patterns) API I want to use in future, but underhood is still hamilton. In the other three files, normal hamilton API is used. 

The graph is(function name is very casual, sorry for that):
```
| prepare | -> | equilibration | -> | production |
|assemble -> cut_struct_up ->| -> |write_script| -> |property_calculate|
```

I omit the details to make code clean, just show three types of tasks. All the tasks submitted to slurm always write down a DSL script first and a `submit.sh`, and call `sbatch submit.sh` to submit the task to slurm. So there is tons of `subprocess.run` used. 

(What I want to do in `molexp` is add helper classes to make it easier for:

- render the DSL script from template
- submit / execute the task
- execute tasks in batches
- retrieve data from batch tasks)