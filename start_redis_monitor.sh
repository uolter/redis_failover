#!/bin/bash

PROGRAM="nohup python redis_monitor.py -p /siac/redis/cluster -z localhost:2181,localhost:2182 -r localhost:6379,localhost:6389,localhost:6399 -s 30"
$PROGRAM &
PID=$!
echo $PID > process.pid
