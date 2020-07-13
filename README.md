# Neton

Distributed reliable key-value store for the most critical data of a distributed system, like zookeeper and etcd.

## Get ready
Install the rocksdb development lib 
```sh
$ sudo apt-get install zstd
$ sudo apt-get install librocksdb-dev
```

## API

*the neton  [api](Documentation/api.md)*

## Start

./restart.sh  will delete snap & entry , recreate distributed server.

./start.sh    go on distributed server using entry & sanp.

./stop.sh     stop distributed server




