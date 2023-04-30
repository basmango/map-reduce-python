
from abc import ABC, abstractmethod
from enum import Enum
from map_reduce.jobs.base_job import BaseMapReduceJob


class InvertedIndexJob(BaseMapReduceJob):
    def map(mapper_in):
        file_id = mapper_in[0]
        lines = mapper_in[1].split("\n")
        words = []
        for line in lines:
            words.extend(line.split(" "))
        words = set(words)
        kv_pairs = []
        for word in words:
            kv_pairs.append((word, file_id))
        return kv_pairs

    def reduce(kv_pairs):
        """
        perform reduce operation on the key value pairs
        """
        kv_dict = {}
        for kv_pair in kv_pairs:
            key = kv_pair[0]
            value = kv_pair[1]
            if key in kv_dict:
                kv_dict[key] += [value]
            else:
                kv_dict[key] = [value]

        return sorted(kv_dict.items(), key=lambda x: x[0])

    def hash(text):
          if text is None:
           return 0
          if type(text) is int:
              test = str(text)
          if len(text) == 0:
           return 0
          
          hash=0
          for ch in text:
           hash = (hash * 317 ^ ord(ch) * 761) & 0xFFFFFFFF
          return hash
        
    def sort(key):
        return key

    def parse_mapper_input(input_dir, sequence_id):
        """
        Reads file Input{sequence_id} from input_dir, tokenize all words and convert to key value pairs of the form (word,1)
        """

        with open(input_dir + "/Input" + str(sequence_id) + ".txt", "r") as f:
            data = f.read()
            # spit on new lines and spaces
            return (sequence_id,data)

    def parse_reducer_input(intermediate_dir, sequence_id, mapper_count):
        """
        Reads the reducer input (intermediate) file and splits it into key value pairs
        """
        kv_pairs = []
        for i in range(1, mapper_count + 1):
            with open(
                intermediate_dir
                + "/Intermediate_"
                + str(i)
                + "_"
                + str(sequence_id)
                + ".txt",
                "r",
            ) as f:
                data = f.read()
                lines = data.split("\n")
                for line in lines:
                    kv_pair = line.split(",")
                    if len(kv_pair) == 2:
                        kv_pairs.append((kv_pair[0][2:-1], int(kv_pair[1][:-1])))

        return kv_pairs

    def write_mapper_output(kv_pairs, intermediate_dir, sequence_id, reducer_count):
        """
        Writes the mapper output to intermediate files
        """
        for i in range(1, reducer_count + 1):
            open(
                intermediate_dir
                + "/Intermediate_"
                + str(sequence_id)
                + "_"
                + str(i)
                + ".txt",
                "a",
            ).close()

        for kv_pair in kv_pairs:
            key = kv_pair[0]
            reducer_id = (InvertedIndexJob.hash(key) % reducer_count) + 1

            with open(
                intermediate_dir
                + "/Intermediate_"
                + str(sequence_id)
                + "_"
                + str(reducer_id)
                + ".txt",
                "a",
            ) as f:
                f.write(str(kv_pair) + "\n")

    def write_reducer_output(kv_pairs, output_dir, sequence_id):
        """
        Writes the reducer output to output files
        """
        open(output_dir + "/Output" + str(sequence_id) + ".txt", "a").close()
        for kv_pair in kv_pairs:
            with open(
                output_dir + "/Output" + str(sequence_id) + ".txt",
                "a",
            ) as f:
                # write in format <word> <count>
                f.write(str(kv_pair[0]) + " " + str(tuple(kv_pair[1])) + "\n")
