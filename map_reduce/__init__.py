import time
import sys
import os
import subprocess
from map_reduce.master import Master
from map_reduce.config_map import ConfigMap, TASKS
import data_classes_pb2_grpc
import data_classes_pb2
import grpc
from concurrent import futures
from map_reduce import reducer
import map_reduce.mapper as mapper
import uuid
import multiprocessing


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

        master_process = self.spawn_master()
        self.map_phase()
        self.reduce_phase()

        master_process.kill()

    def map_phase(self):
        # spawn mappers based on config
        mapper_list = []
        for i in range(self.config.num_mappers):
            # create mapper process
            mapper_instance = mapper.Mapper()
            # use subprocess to spawn mapper process
            mapper_process = multiprocessing.Process(
                target=mapper_instance.run,
            )
            mapper_list += [mapper_process]

            mapper_process.start()
        for i in mapper_list:
            i.join()

    def reduce_phase(self):
        # spawn reducers based on config
        reducer_list = []
        for i in range(self.config.num_reducers):
            # create reducer process
            reducer_instance = reducer.Reducer()
            reducer_process = multiprocessing.Process(
                target=reducer_instance.run,
            )
            reducer_list += [reducer_process]

            reducer_process.start()
        for i in reducer_list:
            i.join()

    def spawn_master(self) -> None:
        master_process = multiprocessing.Process(
            target=self._spawn_master,
        )
        master_process.start()
        return master_process

    def _spawn_master(self) -> None:
        # start master using multi-procesesing
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        data_classes_pb2_grpc.add_masterServicer_to_server(Master(self.config), server)
        server.add_insecure_port("[::]:50051")
        server.start()

        server.wait_for_termination()
