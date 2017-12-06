"""
Multiplying large matrices.
"""

import tensorflow as tf
import time

start_time = time.time()
# Size of matrix and the divisions
M = 100000
d = 100
N = M / d

# number of works
num_works = 5
# create a empty graph
g = tf.Graph()

# make the graph we created as the default graph. All the variables and
# operators we create will be added to this graph
with g.as_default():

    tf.logging.set_verbosity(tf.logging.DEBUG)

    # this sets the random seed for operations on the current graph
    tf.set_random_seed(2048)

    matrices = {} # a container to hold the operators we just created
    for i in range(0, d):
	for j in range(0, d):

            with tf.device("/job:worker/task:%d" % ((i * d + j) % num_works)):
                matrix_name = "matrix-"+str(i)+str(j)
                # create a operator that generates a random_normal tensor of the
                # specified dimensions. By placing this statement here, we ensure
                # that this operator is executed on "/job:worker/task:i". By
                # default, TensorFlow chooses the process to which the client
                # connects to. Feel free to experiment with default or alternate
                # placement strategies.
                matrices[matrix_name] = tf.random_normal([N, N], name=matrix_name)

    # container to hold operators that calculate the traces of individual
    # matrices.
    intermediate_traces = {}
    for i in range(0, d):
	for j in range(0, d):

            with tf.device("/job:worker/task:%d" % ((i * d + j) % num_works)):
                A = matrices["matrix-"+str(i)+str(j)]
                B = matrices["matrix-"+str(j)+str(i)]
                # tf.trace() will create an operator that takes a single matrix
                # as input and calculates it input.
                intermediate_traces["matrix-"+str(i)+str(j)] = tf.trace(tf.matmul(A, B))

    # sum all the traces
    with tf.device("/job:worker/task:0"):
        retval = tf.add_n(intermediate_traces.values())

    config = tf.ConfigProto(log_device_placement=True)
    with tf.Session("grpc://vm-8-1:2222", config=config) as sess:
        result = sess.run(retval)
	print result
        sess.close()
        print "SUCCESS"

end_time = time.time()
print("Time cost: %d" % (end_time - start_time))
