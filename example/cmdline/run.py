import os

from hamilton.execution import executors
from hamilton.experimental import h_cache
from hamilton.plugins import h_tqdm
from hamilton import driver

import funcs
from prototype import cmdline

API_KEY = os.environ["DAGWORKS_API_KEY"]
PROJECT_ID = os.environ["DAGWORKS_PROJECT_ID"]
USER_NAME = os.environ["DAGWORKS_USER_NAME"]


if __name__ == "__main__":
    hamilton_adapters = [
        h_tqdm.ProgressBar(desc="toy-cmdline-dag"),
        h_cache.CachingGraphAdapter("./cache"),
    ]

    if API_KEY and PROJECT_ID and USER_NAME:
        from dagworks import adapters

        tracker = adapters.DAGWorksTracker(
            username=USER_NAME,
            api_key=API_KEY,
            project_id=int(PROJECT_ID),
            dag_name="toy-cmdline-dag",
            tags={"env": "local"},  # , "TODO": "add_more_tags_to_find_your_run_later"},
        )
        hamilton_adapters.append(tracker)

    dr = (
        driver.Builder()
        .enable_dynamic_execution(allow_experimental_mode=True)
        .with_execution_manager(
            cmdline.CMDLineExecutionManager(
                executors.SynchronousLocalTaskExecutor(),
                executors.MultiThreadingExecutor(5),
            )
        )
        .with_modules(funcs)
        .with_adapters(*hamilton_adapters)
        .build()
    )
    dr.display_all_functions("graph.dot")
    print(dr.list_available_variables())
    # for var in dr.list_available_variables():
    #     print(dr.execute([var.name], inputs={"start": "hello"}))
    print(dr.execute(["echo_3"], inputs={"start": "hello"}))
