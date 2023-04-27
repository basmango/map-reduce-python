# mapper class in map reduce, takes Job enum during initialization
# from map_reduce.config_map import ConfigMap, TASKS
import uuid
import data_classes_pb2_grpc as master_pb2_grpc
import data_classes_pb2 as master_pb2
import grpc
from map_reduce.config_map import ConfigMap, TASKS


class Mapper:
    def __init__(self) -> None:
        # generate random UUID
        self.uuid = str(uuid.uuid4())
        self.sequence_id = -1
        self.kv_pairs = []
        self.config = None

    def run(self) -> None:
        """
        sends grpc request to master server with uuid and registration type,
        """
        # create stub

        with grpc.insecure_channel("localhost:50051") as channel:
            stub = master_pb2_grpc.masterStub(channel)
            # create request
            request = master_pb2.RegisterRequest(
                uuid=self.uuid,
                registration_type="mapper",
            )
            response = stub.register(request)

            self.config = ConfigMap(
                num_mappers=response.num_mappers,
                num_reducers=response.num_reducers,
                task=TASKS[response.task],
                input_dir=response.input_directory,
                output_dir=response.output_directory,
                intermediate_dir=response.intermediate_directory,
            )
            self.sequence_id = response.worker_sequence_id

            job = self.config.task.value
            print((self.config.input_dir, self.sequence_id))
            self.kv_pairs = job.read_and_split_mapper_input(
                self.config.input_dir, self.sequence_id
            )

            mapper_result = job.map(self.kv_pairs)

            job.write_mapper_output(
                mapper_result,
                self.config.intermediate_dir,
                self.sequence_id,
                self.config.num_reducers,
            )

            

if __name__ == "__main__":
    mapper = Mapper()
    mapper.run()
    print("mapper started")
