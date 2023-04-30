# abstacct class for map reduce job, defines the following : map,reduce and hash and sort functions

from abc import ABC, abstractmethod
from enum import Enum
from map_reduce.jobs.base_job import BaseMapReduceJob
import json

class NaturalJoinJob(BaseMapReduceJob):
    table1_cols = []
    table2_cols = []
    common_col_index = (0, 0)

    def map(mapper_in):
        table1_arr = mapper_in[0]
        table2_arr = mapper_in[1]

        mapper_out_dict = {}

        # you have the common column stored already, you know all the columns present in both the tables, you have the values for all the columns of each table in table1_arr and table2_arr
        # iterate over all columns of table 1, and store the values in a dictionary with the key as the common column value, and the value as the entire row, also store the table number
        # iterate over all columns of table 2, and store the values in a dictionary with the key as the common column value, and the value as the entire row, also store the table number
        # if the key is already present in the dictionary, append the value to the list of values for that key

        for row in table1_arr:
            key = row[NaturalJoinJob.common_col_index[0]]
            if key not in mapper_out_dict:
                mapper_out_dict[key] = []
            mapper_out_dict[key].append((1, row))

        for row in table2_arr:
            key = row[NaturalJoinJob.common_col_index[1]]
            if key not in mapper_out_dict:
                mapper_out_dict[key] = []
            mapper_out_dict[key].append((2, row))

        # convert dict to list of key value pairs
        mapper_out = []
        for key in mapper_out_dict:
            mapper_out.append((key, mapper_out_dict[key]))

        return mapper_out

    def write_mapper_output(kv_pairs, intermediate_dir, sequence_id, reducer_count):
        """
        Writes the mapper output to intermediate files
        """
        # in intermediate directory , first create partitions of the form intermediate_dir_sequence_id_reducer_id, each partition should contain in the first 3 lines, the column names of the two tables and the common column ids respectively
        # then in the following lines, write the data
        # hash and mod the key to get the reducer id and write to the corresponding file

        # create partitions
        partitions = []
        for i in range(reducer_count):
            partitions.append(
                open(
                    intermediate_dir
                    + "/Intermediate_"
                    + str(sequence_id)
                    + "_"
                    + str(i + 1)
                    + ".txt",
                    "w",
                )
            )

            # write column names
            partitions[i].write(" ".join(NaturalJoinJob.table1_cols) + "\n")
            partitions[i].write(" ".join(NaturalJoinJob.table2_cols) + "\n")
            # write common column ids

            partitions[i].write(
                str(NaturalJoinJob.common_col_index[0])
                + " "
                + str(NaturalJoinJob.common_col_index[1])
                + "\n"
            )

        # write data
        for kv_pair in kv_pairs:
            key = kv_pair[0]
            value = kv_pair[1]
            reducer_id = NaturalJoinJob.hash(key) % reducer_count
            # write this part as a json string
            partitions[reducer_id].write(json.dumps((key, value)) + "\n")
        # close partitions
        for i in range(reducer_count):
            partitions[i].close()

    def reduce(kv_pairs):
        """
        kv pairs contains the key value pairs for a particular reducer
        the key is the common column value
        the value is a list of tuples, each tuple contains the table number and the row data

        this function must return a list of tuples representing the joined table
        """
        kv_dict = {}
        for kv_pair in kv_pairs:
            key = kv_pair[0]
            value = kv_pair[1]
            if key not in kv_dict:
                kv_dict[key] = []
            kv_dict[key]+=value

        # now, for each key, make a cartesian product of the values
        reducer_out = []
        for key in kv_dict:
            table1_rows = []
            table2_rows = []
            value_arr = kv_dict[key]
            for value in value_arr:
                if value[0] == 1:
                    table1_rows.append(value[1])
                else:
                    table2_rows.append(value[1])
            
            # make cartesian product
            for table1_row in table1_rows:

                for table2_row in table2_rows:
                    table2_row = table2_row[:NaturalJoinJob.common_col_index[1]] + table2_row[NaturalJoinJob.common_col_index[1] + 1:]

                    reducer_out.append(table1_row + table2_row)


        return reducer_out
    

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
        Reads the mapper input file and splits it into key value pairs
        """
        # in input dir input dir, there are two files, one is the left table, the other is the right table
        # the left table is the first file, the right table is the second file
        # the first line of each file is the column names
        # the rest of the lines are the data
        # the key is the column name, the value is the data
        # format of table files is inputi_tablej.txt where i is table number and j is mapper number

        # read both  files, for table 1 and 2

        table1_file = input_dir + "/input" + str(sequence_id) + "_table1.txt"
        table2_file = input_dir + "/input" + str(sequence_id) + "_table2.txt"

        # read table 1

        with open(table1_file, "r") as f:
            table1_data = f.read()
            # split on new lines
            table1_lines = table1_data.split("\n")
            # split on spaces
            NaturalJoinJob.table1_cols = table1_lines[0].split(",")
            # remove the first line
            table1_lines.pop(0)

            # convert all rows to array of tuples
            mapper_table1_input = []
            for line in table1_lines:
                mapper_table1_input.append(tuple(line.split(",")))

        # read table 2

        with open(table2_file, "r") as f:
            table2_data = f.read()
            # split on new lines
            table2_lines = table2_data.split("\n")
            # split on spaces
            # remove the first line
            NaturalJoinJob.table2_cols = table2_lines[0].split(",")
            table2_lines.pop(0)

            # convert all rows to array of tuples
            mapper_table2_input = []
            for line in table2_lines:
                mapper_table2_input.append(tuple(line.split(",")))

        # iterate over table 1 and table 2 column names and find the common column, store it in common_col_index

        for i in range(len(NaturalJoinJob.table1_cols)):
            for j in range(len(NaturalJoinJob.table2_cols)):
                if NaturalJoinJob.table1_cols[i] == NaturalJoinJob.table2_cols[j]:
                    NaturalJoinJob.common_col_index = (i, j)
                    break
            if NaturalJoinJob.common_col_index != (0, 0):
                break

        return (mapper_table1_input, mapper_table2_input)

    def parse_reducer_input(intermediate_dir, sequence_id, mapper_count):
        """
        Reads the reducer input (intermediate) file and splits it into key value pairs
        """

        # in intermediate directory , first create partitions of the form intermediate_dir_sequence_id_reducer_id, each partition should contain in the first 3 lines, the column names of the two tables and the common column ids respectively
        # then in the following lines, write the data
        # hash and mod the key to get the reducer id and write to the corresponding file

        # read the intermediate file
        reducer_input = []
        for mapper_id in range(1, mapper_count + 1):
            intermediate_file = (
                intermediate_dir
                + "/Intermediate_"
                + str(mapper_id)
                + "_"
                + str(sequence_id)
                + ".txt"
            )
            with open(intermediate_file, "r") as f:
                intermediate_data = f.read()
                # split on new lines
                intermediate_lines = intermediate_data.split("\n")
                # split on spaces
                # remove the first 3 lines
                # read the first 3 lines, and populate appropriate variables
                NaturalJoinJob.table1_cols = intermediate_lines[0].split(" ")
                NaturalJoinJob.table2_cols = intermediate_lines[1].split(" ")
                NaturalJoinJob.common_col_index = tuple(
                    intermediate_lines[2].split(" ")
                )
                # conver common col index to int
                NaturalJoinJob.common_col_index = (
                    int(NaturalJoinJob.common_col_index[0]),
                    int(NaturalJoinJob.common_col_index[1]),
                )

                intermediate_lines.pop(0)
                intermediate_lines.pop(0)
                intermediate_lines.pop(0)

                # convert all rows to array of tuples
                for line in intermediate_lines:
                    if line == "":
                        continue
                    val = json.loads(line)
                    # append to reducer input
                    reducer_input.append(val)
        

        return reducer_input

    def write_reducer_output(kv_pairs, output_dir, sequence_id):
        """
        Writes the reducer output to the output file
        """

        output_file = output_dir + "/Output" + str(sequence_id) + ".txt"

        # write the joined column names
        with open(output_file, "w") as f:
            f.write(",".join(NaturalJoinJob.table1_cols))
            f.write(",")
            # remove the common column from table 2 cols
            table2_cols = NaturalJoinJob.table2_cols[:NaturalJoinJob.common_col_index[1]] + NaturalJoinJob.table2_cols[NaturalJoinJob.common_col_index[1] + 1:]
            f.write(",".join(table2_cols))

            f.write("\n")
            # write the data
            for kv_pair in kv_pairs:
                f.write(",".join(kv_pair))
                f.write("\n")