#!/bin/sh
ulimit -c unlimited
killall -9 neton
rm -rf entry.log*
rm -rf snap.log*
rm -rf neton.log*
rm -rf rocks.db* 
rm -rf snap-*;
rm -rf wal-*; 
./neton -p "./config/neton1.conf" &
./neton -p "./config/neton2.conf" &
./neton -p "./config/neton3.conf" &
