#!/bin/bash

protoc --plugin="/home/zhyc/share/pt_cpp/protobuf-d/build/protoc-gen-d" --d_out=./../source/ -I./ etcdserverpb.kv.proto rpc.proto 
protoc --plugin=protoc-gen-grpc=grpc_dlang_plugin --grpc_out=./../source/etcdserverpb -I./ rpc.proto
