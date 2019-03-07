#!/bin/bash

protoc --plugin="/home/zhyc/share/pt_cpp/protobuf-d/build/protoc-gen-d" --d_out=./../source/ -I./  neton.proto 
protoc --plugin=protoc-gen-grpc=grpc_dlang_plugin --grpc_out=./../source/neton/protocol -I./ neton.proto
