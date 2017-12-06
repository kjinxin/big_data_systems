#!/bin/sh

echo "Start: " $(date +%s)

$SPARK_HOME/bin/spark-submit /home/ubuntu/PartC/PartCQuestion-2.py /web-BerkStan.txt 10 32

echo "End: " $(date +%s)
