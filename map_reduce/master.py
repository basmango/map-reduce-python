import multiprocessing
from concurrent import futures
import grpc
import data_classes_pb2_grpc
import data_classes_pb2
from map_reduce import reducer


class Master(data_classes_pb2_grpc.masterServicer):
    mapper_dict = {}
    reducer_dict = {}
    mapper_sequence_id = 1
    reducer_sequence_id = 1
    config = None

    def __init__(self, config) -> None:
        super().__init__()
        self.config = config

    def register(self, request, context):
        # extract UUID from request
        uuid = request.uuid
        # check enum REGISTER_TYPE
        register_type = request.registration_type

        response_sequence_id = -1

        if register_type == "mapper":
            response_sequence_id = self.mapper_sequence_id
            self.mapper_sequence_id += 1
        elif register_type == "reducer":
            response_sequence_id = self.reducer_sequence_id
            self.reducer_sequence_id += 1

        register_response = data_classes_pb2.RegisterResponse(
            num_mappers=self.config.num_mappers,
            num_reducers=self.config.num_reducers,
            task=str(self.config.task.name),
            input_directory=self.config.input_dir,
            output_directory=self.config.output_dir,
            intermediate_directory=self.config.intermediate_dir,
            worker_sequence_id=response_sequence_id,
        )
        return register_response
