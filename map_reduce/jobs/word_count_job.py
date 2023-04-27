# abstacct class for map reduce job, defines the following : map,reduce and hash and sort functions

from abc import ABC, abstractmethod
from enum import Enum
from map_reduce.jobs.base_job import BaseMapReduceJob


class WordCountJob(BaseMapReduceJob):
    def map(kv_pairs):
        print("ml")

    def reduce(key):
        pass

    def hash(key):
        return hash(key)

    def sort(key):
        return key

    def read_and_split_mapper_input(input_dir, sequence_id):
        """
        Reads the mapper input file and splits it into key value pairs
        """
        pass

    def read_and_split_reducer_input(intermediate_dir, sequence_id):
        """
        Reads the reducer input (intermediate) file and splits it into key value pairs
        """
        pass
