This directory contains source code for applications in Part-C
For Question1, we split window implement of disjoint (Tumbling Window) and non-disjoint (Sliding Window) cases into 
PartCQuestion_1_1.java and PartCQuestion_1_2.java respectively. For Question 2, we lateness allowances are implemented
in PartCQuestion_2.java. Please note that, we hard coded the lateness of 100s in PartCQuestion_2.java.

All source code files are located in the Flink project, please go to /home/ubuntu/quickstart/src/main/java/org/myorg/quickstart to check them.
We already provide script files in /home/ubuntu/grader_assign2 to run applications for Qeustion 1 and Question 2, please check 
PartCQuestion1-1.sh, PartCQuestion1-2.sh, PartCQuestion1.sh, and PartCQuestion2.sh for detailed information.

1. To run application for Question 1, there are two ways:
    (a) run Tumbling Window and Sliding Window applications seperately:
        
        ```
        cd /home/ubuntu/grader_assign2
        ./PartCQuestion1-1.sh
        ```
        
        These commands will run Tumbling Window with disjoint and print the output on the console
        
        While commands:
        
        ```
        cd /home/ubuntu/grader_assign2
        ./PartCQuestion1-2.sh
        ```
        
        will run Sliding Window application and print the output

    (b) run both applications one after another:
        
        ```
        cd /home/ubuntu/grader_assign2
        ./PartCQuestion1.sh
        ```
  
    If you look into this script, you will find it just execute those two seperate scripts one by one and print out the results.

2. To run application for Question 2 (only with allowance at 100s)
    Following the commands
    
    ```
    cd /home/ubuntu/grader_assign2
    ./PartCQuestion2.sh
    ```
    
    Similar to previous scripts, this will also print the output on the console
