# abstacct class for map reduce job, defines the following : map,reduce and hash and sort functions

from abc import ABC,abstractmethod
from enum import Enum


class BaseMapReduceJob():

    def map( key: str, value: str):
        pass

    def reduce( key, values):
        pass

    def hash( key) :
        return hash(key)
    

    def sort( key) :
        return key

    def read_and_split_mapper_input( input_file: str):
        """
        Reads the mapper input file and splits it into key value pairs
        """
        pass

    def read_and_split_reducer_input( input_file: str):
        """
        Reads the reducer input (intermediate) file and splits it into key value pairs
        """
        pass

    
    
