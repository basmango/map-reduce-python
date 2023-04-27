from map_reduce.jobs.base_job import BaseMapReduceJob
from map_reduce.jobs.inverted_index_job import InvertedIndexJob
from map_reduce.jobs.word_count_job import WordCountJob
from map_reduce.jobs.natural_join_job import NaturalJoinJob

from enum import Enum


class TASKS(Enum):
    INVERTED_INDEX = InvertedIndexJob
    WORD_COUNT = WordCountJob
    NATURAL_JOIN = NaturalJoinJob


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
