#!/bin/sh
ulimit -c unlimited
#rm -rf entry.log*
rm -f neton.log
./neton -p "./config/neton1.conf" &
./neton -p "./config/neton2.conf" &
./neton -p "./config/neton3.conf" &
