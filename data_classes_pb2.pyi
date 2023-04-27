from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class RegisterRequest(_message.Message):
    __slots__ = ["registration_type", "uuid"]
    REGISTRATION_TYPE_FIELD_NUMBER: _ClassVar[int]
    UUID_FIELD_NUMBER: _ClassVar[int]
    registration_type: str
    uuid: str
    def __init__(
        self, uuid: _Optional[str] = ..., registration_type: _Optional[str] = ...
    ) -> None: ...

class RegisterResponse(_message.Message):
    __slots__ = [
        "input_directory",
        "intermediate_directory",
        "num_mappers",
        "num_reducers",
        "output_directory",
        "task",
        "worker_sequence_id",
    ]
    INPUT_DIRECTORY_FIELD_NUMBER: _ClassVar[int]
    INTERMEDIATE_DIRECTORY_FIELD_NUMBER: _ClassVar[int]
    NUM_MAPPERS_FIELD_NUMBER: _ClassVar[int]
    NUM_REDUCERS_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_DIRECTORY_FIELD_NUMBER: _ClassVar[int]
    TASK_FIELD_NUMBER: _ClassVar[int]
    WORKER_SEQUENCE_ID_FIELD_NUMBER: _ClassVar[int]
    input_directory: str
    intermediate_directory: str
    num_mappers: int
    num_reducers: int
    output_directory: str
    task: str
    worker_sequence_id: int
    def __init__(
        self,
        num_mappers: _Optional[int] = ...,
        num_reducers: _Optional[int] = ...,
        task: _Optional[str] = ...,
        input_directory: _Optional[str] = ...,
        output_directory: _Optional[str] = ...,
        intermediate_directory: _Optional[str] = ...,
        worker_sequence_id: _Optional[int] = ...,
    ) -> None: ...

class SimpleString(_message.Message):
    __slots__ = ["str"]
    STR_FIELD_NUMBER: _ClassVar[int]
    str: str
    def __init__(self, str: _Optional[str] = ...) -> None: ...
