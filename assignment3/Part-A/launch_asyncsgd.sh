#!/bin/bash

source tfdefs.sh
echo "terminating the unclosed jobs on cluster ..."
terminate_cluster
echo "done termination, training process begin ..."
start_cluster startserver.py
# start multiple clients
nohup python asyncsgd.py --task_index=0 > async-0.out
echo "sleep here!"
sleep 10 #it for variable to be initialized
echo "done sleep!"
nohup python asyncsgd.py --task_index=1 > async-1.out
nohup python asyncsgd.py --task_index=2 > async-2.out
nohup python asyncsgd.py --task_index=3 > async-3.out
nohup python asyncsgd.py --task_index=4 > async-4.out
