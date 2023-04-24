from enum import Enum
import time
import sys
import os
import subprocess
import multiprocessing
class TASKS(Enum):
    INVERTED_INDEX = 1
    WORD_COUNT = 2
    NATURAL_JOIN = 3


class ConfigMap:
    def __init__(
        self,
        num_mappers: int,
        num_reducers: int,
        task: TASKS,
        input_dir: str,
        output_dir: str,
        intermediate_dir: str,
    ) -> None:
        self.num_mappers = num_mappers
        self.num_reducers = num_reducers
        self.task = task
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.intermediate_dir = intermediate_dir

    def __str__(self) -> str:
        return f"Number of mappers: {self.num_mappers} \nNumber of reducers: {self.num_reducers} \nTask: {self.task} \nInput directory: {self.input_dir} \nOutput directory: {self.output_dir} \nIntermediate directory: {self.intermediate_dir}"



class MapReduce:
    def __init__(
        self,
        num_mappers: int,
        num_reducers: int,
        task: TASKS,
        input_dir: str,
        output_dir: str,
        intermediate_dir: str,
    ) -> None:
        self.config = ConfigMap(
            num_mappers, num_reducers, task, input_dir, output_dir, intermediate_dir
        )

    def run(self) -> None:
        print("Starting map reduce job with configuration:")
        print(self.config)

        # start master node process
        self._spawn_master()

    def _spawn_master(self) -> None:
        # spawn master process
        master = subprocess.Popen(
            [
                "python3",
                "master.py",
                str(self.config.num_mappers),
                str(self.config.num_reducers),
                str(self.config.task.value),
                self.config.input_dir,
                self.config.output_dir,
                self.config.intermediate_dir,
            ]
        )
        master.wait()
        print("Master process finished")

    def _spawn_mappers(self) -> None:
        pass

    def _spawn_reducers(self) -> None:
        pass



