#!/bin/bash

command=$1

if [ $command = "start-jupyter" ]; then
  jupyter notebook --ip=0.0.0.0 --port=8888 --allow-root --no-browser --NotebookApp.token='' --NotebookApp.password=''
fi

tail -f /dev/null
