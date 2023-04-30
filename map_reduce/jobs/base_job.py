# abstacct class for map reduce job, defines the following : map,reduce and hash and sort functions

from abc import ABC, abstractmethod
from enum import Enum


class BaseMapReduceJob:
    def map(kv_pairs):
        pass

    def reduce(k_pairs):
        pass

    def hash(key):
        return hash(key)

    def sort(array):
        return array

    def parse_mapper_input(input_dir, sequence_id):
        """
        Reads the mapper input file and splits it into key value pairs
        """
        # read file 'Input{sequence_id}' from input_dir, tokenize all words and convert to key value pairs
        pass

    def parse_reducer_input(intermediate_dir, sequence_id):
        """
        Reads the reducer input (intermediate) file and splits it into key value pairs
        """
        # read file 'Input{sequence_id}' from in

    def write_mapper_output(kv_pairs, intermediate_dir, sequence_id, reducer_count):
        """
        Writes the mapper output to intermediate files
        """
        # in intermediate directory , first create empty partitions of the form intermediate_dir_sequence_id_reducer_id
        # hash and mod the key to get the reducer id and write to the corresponding file

        # create empty partitions
        for i in range(1, reducer_count + 1):
            open(
                intermediate_dir + "/Intermediate_" + str(sequence_id) + "_" + str(i),
                "a",
            ).close()

    def write_reducer_output(kv_pairs, output_dir, sequence_id):
        """
        Writes the reducer output to output files
        """
        # write to output directory
        # create empty partitions
        open(output_dir + "/Output" + str(sequence_id) + ".txt", "a").close()
