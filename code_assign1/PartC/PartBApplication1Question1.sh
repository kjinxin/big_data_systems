#!/bin/sh
echo "Start: " $(date +%s)
cat /proc/net/dev > output_c_3/net_start_${i}_vm-8-1
cat /proc/diskstats > output_c_3/disk_start_${i}_vm-8-1
for server in vm-8-2 vm-8-3 vm-8-4 vm-8-5
do
  ssh $server 'cat /proc/net/dev' > output_c_3/net_start_${i}_${server}
  ssh $server 'cat /proc/diskstats' > output_c_3/disk_start_${i}_${server}
done

$SPARK_HOME/bin/spark-submit --class PartBApplication1Question1 /home/ubuntu/assign3_graphx/spark_graphx/target/spark_graphx-1.0.jar /soc-LiveJournal1.txt 20

cat /proc/net/dev > output_c_3/net_end_${i}_vm-8-1
cat /proc/diskstats > output_c_3/disk_end_${i}_vm-8-1

for server in vm-8-2 vm-8-3 vm-8-4 vm-8-5
do
  ssh $server 'cat /proc/net/dev' > output_c_3/net_end_${i}_${server}
  ssh $server 'cat /proc/diskstats' > output_c_3/disk_end_${i}_${server}
done

echo "End: " $(date +%s)
