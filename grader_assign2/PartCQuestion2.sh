#!/bin/bash
cd ~/quickstart
~/software/flink-1.3.2/bin/flink run -c org.myorg.quickstart.PartCQuestion_2 ~/quickstart/target/quickstart-0.1.jar
for i in $(seq 1 5);
  do
  scp vm-8-${i}:~/hw2_partc_dir/part_c_out_disjoint_allowlateness_100s ~/ 2>/dev/null
  done
echo "PartC-Q Only show lateness of 100s here : "
cat ~/part_c_out_disjoint_allowlateness_100s
