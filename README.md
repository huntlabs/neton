# Neton

Distributed reliable key-value store for the most critical data of a distributed system, like zookeeper and etcd.


# Dependencies

raft lib @ https://github.com/huntlabs/raft

network dreactor lib @ https://github.com/zhangyuchun/dreactor

A D binding to rocksdb @ https://github.com/b1naryth1ef/rocksdb

RocksDB lib @ https://github.com/facebook/rocksdb

# Api

*the neton  [api](Documentation/api.md)*

# Start

./restart.sh  will delete snap & entry , recreate distributed server.

./start.sh    go on distributed server using entry & sanp.

./stop.sh     stop distributed server




