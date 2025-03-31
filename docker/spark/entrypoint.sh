#!/bin/bash

command=$1

if [ $command = "start-master" ]; then
  /opt/spark/sbin/start-master.sh
elif [ $command = "start-worker" ]; then
  /opt/spark/sbin/start-master.sh
elif [ $command = "start-history-server" ]; then
  /opt/spark/sbin/start-history-server.sh
fi

tail -f /dev/null
