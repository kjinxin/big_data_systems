#!/bin/bash

# tfdefs.sh has helper function to start process on all VMs
# it contains definition for start_cluster and terminate_cluster
source tfdefs.sh

echo "terminating the unclosed jobs on cluster ..."
terminate_cluster
echo "done termination, training process begin ..."

# startserver.py has the specifications for the cluster.
start_cluster startserver.py

echo "Executing the distributed tensorflow job from synchronoussgd.py"
# bigmatrixmultiplication.py is a client that can run jobs on the cluster.
# please read bigmatrixmultiplication.py to understand the steps defining a Graph and
# launch a session to run the Graph
python synchronoussgd.py

# defined in tfdefs.sh to terminate the cluster
terminate_cluster
