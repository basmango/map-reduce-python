#!/usr/bin/env python
# python cli for performing map reduce

from glacier import glacier
from map_reduce import MapReduce
from map_reduce.config_map import TASKS
import os, sys


def test() -> None:
    pass


# run takes number of mappers, number of reducers, directory for mapper inputs
def run(
    _num_mappers: int,
    _num_reducers: int,
    _task: str,
    _input_dir: str,
    output_dir: str = "NA",
    intermediate_dir: str = "NA",
) -> None:
    if output_dir == "NA":
        output_dir = _input_dir

    if intermediate_dir == "NA":
        intermediate_dir = _input_dir

    # perform task checking
    task = _task.upper()
    if task not in TASKS.__members__:
        raise ValueError(
            f"Task not supported, supported tasks are: {list(TASKS.__members__.keys())}"
        )
    else:
        # set task to enum task of same key name
        task = TASKS[task]

    # check if input_dir exists
    if not os.path.isdir(_input_dir):
        raise ValueError("Input directory does not exist")

    # check if output_dir exists
    if not os.path.isdir(output_dir):
        raise ValueError("Output directory does not exist")

    # check if intermediate_dir exists
    if not os.path.isdir(intermediate_dir):
        raise ValueError("Intermediate directory does not exist")

    # check if num_mappers is a positive integer
    if not isinstance(_num_mappers, int) or _num_mappers < 1:
        raise ValueError("Number of mappers must be a positive integer")

    # check if num_reducers is a positive integer
    if not isinstance(_num_reducers, int) or _num_reducers < 1:
        raise ValueError("Number of reducers must be a positive integer")

    mr = MapReduce(
        _num_mappers, _num_reducers, task, _input_dir, output_dir, intermediate_dir
    )
    mr.run()


if __name__ == "__main__":
    # handle execution

    try:
        glacier([test, run])
    except Exception as e:
        print(e)
        sys.exit(1)
