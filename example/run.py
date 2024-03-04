from pathlib import Path
import build
import eq
import tg

from hamilton import base, driver
from hamilton.io.materialization import to
from hamilton.experimental.h_cache import CachingGraphAdapter
from hamilton.plugins import h_experiments, matplotlib_extensions, pandas_extensions  # noqa: F401

tracker_hook = h_experiments.ExperimentTracker(
    experiment_name="exp",
    base_directory="./experiments",
)

dr = (
    driver.Builder()
    .with_modules(build, eq, tg)
    # .with_config(config)
    .with_adapters(tracker_hook, CachingGraphAdapter('.cache'))
    .build()
)

inputs = dict({
    # 'work_dir': ''
    'repeat_unit': ['N', 'M'],
    'repeat': 1,
    'n_chains': 20,
    'density': 0.005,
})

materializers = [
    to.pickle(
        id="after_build",
        dependencies=['submit'],
        path="/proj/snic2021-5-546/users/x_jicli/exp/.cache/to_lammps.pickle",
    )
]

for repeat_unit in ['NMNMP', 'NMNMNMP']:
    for repeat in [1, 4, 8]:
        inputs['repeat_unit'] = list(repeat_unit)
        inputs['repeat'] = repeat

        inputs['work_dir'] = f"{''.join(inputs['repeat_unit'])}x{inputs['repeat']}"
        Path(inputs['work_dir']).mkdir(exist_ok=True)
        dr.visualize_materialization(
            *materializers,
            inputs=inputs,
            output_file_path=f"{tracker_hook.run_directory}/dag",
            render_kwargs=dict(view=False, format="png"),
        )
        dr.materialize(*materializers, inputs=inputs)