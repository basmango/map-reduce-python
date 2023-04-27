python -m grpc_tools.protoc -I . --python_out=. --pyi_out=. --grpc_python_out=. data_classes.proto 
protoc --experimental_allow_proto3_optional -I=. --python_out=.  ./data_classes.proto
