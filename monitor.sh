#!/bin/sh
# Start and stop script

PID_FILE="process.pid"

PROGRAM="nohup python start_monitor.py -p /siac/redis/cluster -z localhost:2181,localhost:2182 -r localhost:6379,localhost:6389,localhost:6399 -s 30"

function quit_monitors {
	pid="$(cat $PID_FILE)"

	for child in $(ps -o pid,ppid -ax | awk "{ if ( \$2 == $pid ) { print \$1 }}")
	do
  		echo "Killing child process $child because ppid = $pid"
  		kill $child
	done

	echo "Killing child process ppid = $pid"
	kill $pid
}

 
case "$1" in
start)
    # start redis monitor
    $PROGRAM &
	PID=$!
	echo $PID > process.pid
	
;;
stop)
   # stop zookeeper
    
    quit_monitors
;;


*)
        echo "Usage: $0 {start|stop}"
        exit 1
esac
