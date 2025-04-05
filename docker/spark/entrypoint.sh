#!/bin/bash

command=$1

if [ $command = "start-master" ]; then
  /opt/spark/sbin/start-master.sh -p $SPARK_MASTER_PORT
elif [ $command = "start-worker" ]; then
  /opt/spark/sbin/start-worker.sh spark://$SPARK_MASTER_HOST:$SPARK_MASTER_PORT
elif [ $command = "start-history-server" ]; then
  /opt/spark/sbin/start-history-server.sh
fi

tail -f /dev/null
