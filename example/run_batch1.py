"""
Option 1: Use hamilton within Hamilton.
"""

from pathlib import Path
import build
import eq
import tg

from hamilton import driver
from hamilton.io.materialization import to
from hamilton.experimental.h_cache import CachingGraphAdapter
from hamilton.plugins import h_experiments, matplotlib_extensions, pandas_extensions  # noqa: F401
from hamilton.function_modifiers import tag, value, parameterize

from molexp.cmdline import CMDLineExecutionManager
from hamilton.execution import executors


# could use @resolve to dynamically create this via passed in configuration.
# this is statically declared here for now.
cross_product = {
    f"{ru}x{r}": {"repeat_unit": value(ru), "repeat": value(r)}
    for ru in ["NMNMP", "NMNMNMP"]
    for r in [1, 4, 8]
}


@parameterize(**cross_product)
@tag(cmdline="slurm")
def experiment(repeat_unit: str, repeat: int) -> dict:
    """Node to run an experiment."""
    tracker_hook = h_experiments.ExperimentTracker(
        experiment_name="exp",
        base_directory="./experiments",
    )

    execution_manager = CMDLineExecutionManager(
        executors.SynchronousLocalTaskExecutor(),
        executors.SynchronousLocalTaskExecutor(),
    )

    dr = (
        driver.Builder()
        .with_modules(build, eq, tg)
        # .with_config(config)
        .with_adapters(tracker_hook, CachingGraphAdapter(".cache"))
        .enable_dynamic_execution(allow_experimental_mode=True)
        .with_execution_manager(execution_manager)
        .build()
    )

    inputs = dict(
        {
            "n_chains": 20,
            "density": 0.005,
        }
    )

    materializers = [
        to.pickle(
            id="after_build",
            dependencies=["submit"],
            path="/proj/snic2021-5-546/users/x_jicli/exp/.cache/to_lammps.pickle",
        )
    ]

    inputs["repeat_unit"] = list(repeat_unit)
    inputs["repeat"] = repeat

    inputs["work_dir"] = f"{''.join(inputs['repeat_unit'])}x{inputs['repeat']}"
    Path(inputs["work_dir"]).mkdir(exist_ok=True)
    dr.visualize_materialization(
        *materializers,
        inputs=inputs,
        output_file_path=f"{tracker_hook.run_directory}/dag",
        render_kwargs=dict(view=False, format="png"),
    )
    meta, _ = dr.materialize(*materializers, inputs=inputs)
    return meta


if __name__ == "__main__":
    import run_batch1

    # tracker_hook = h_experiments.ExperimentTracker(
    #     experiment_name="exp",
    #     base_directory="./experiments",
    # )

    execution_manager = CMDLineExecutionManager(
        executors.SynchronousLocalTaskExecutor(),
        executors.MultiThreadingExecutor(20),
    )

    dr = (
        driver.Builder()
        .with_modules(run_batch1)
        # .with_config(config)
        .enable_dynamic_execution(allow_experimental_mode=True)
        .with_execution_manager(execution_manager)
        .build()
    )
    dr.display_all_functions("run_batch1.png")
    dr.execute([dr.list_available_variables()])
