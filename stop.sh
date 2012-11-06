#!/bin/bash

pid="$(cat process.pid)"

for child in $(ps -o pid,ppid -ax | awk "{ if ( \$2 == $pid ) { print \$1 }}")
do
  echo "Killing child process $child because ppid = $pid"
  kill $child
done

echo "Killing child process ppid = $pid"
kill $pid