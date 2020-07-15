#!/bin/bash

protoc --plugin=/usr/local/bin/protoc-gen-d --d_out=./../source/ -I./  neton.proto 
protoc --plugin=protoc-gen-grpc=/usr/local/bin/grpc_dlang_plugin --grpc_out=./../source/neton/protocol -I./ neton.proto
