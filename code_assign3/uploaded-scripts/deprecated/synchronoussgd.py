import tensorflow as tf
import time

# number of features in the criteo dataset after one-hot encoding
num_features = 33762578
# number of works
num_works = 5
# iteration num
num_train_iters = 100000
# test number
num_test_iters = 10000
# eta
eta = 0.01
# model eval freq
eval_freq = 10
# Here, we will show how to include reader operators in the TensorFlow graph.
# These operators take as input list of filenames from which they read data.
# On every invocation of the operator, some records are read and passed to the
# downstream vertices as Tensors


g = tf.Graph()

input_data = [
	["/home/ubuntu/criteo-tfr/tfrecords00", "/home/ubuntu/criteo-tfr/tfrecords01", "/home/ubuntu/criteo-tfr/tfrecords02", "/home/ubuntu/criteo-tfr/tfrecords03", "/home/ubuntu/criteo-tfr/tfrecords04"],
	["/home/ubuntu/criteo-tfr/tfrecords05", "/home/ubuntu/criteo-tfr/tfrecords06", "/home/ubuntu/criteo-tfr/tfrecords07", "/home/ubuntu/criteo-tfr/tfrecords08", "/home/ubuntu/criteo-tfr/tfrecords09"],
	["/home/ubuntu/criteo-tfr/tfrecords10", "/home/ubuntu/criteo-tfr/tfrecords11", "/home/ubuntu/criteo-tfr/tfrecords12", "/home/ubuntu/criteo-tfr/tfrecords13", "/home/ubuntu/criteo-tfr/tfrecords14"],
	["/home/ubuntu/criteo-tfr/tfrecords15", "/home/ubuntu/criteo-tfr/tfrecords16", "/home/ubuntu/criteo-tfr/tfrecords17", "/home/ubuntu/criteo-tfr/tfrecords18", "/home/ubuntu/criteo-tfr/tfrecords19"],
	["/home/ubuntu/criteo-tfr/tfrecords20", "/home/ubuntu/criteo-tfr/tfrecords21"],
	["/home/ubuntu/criteo-tfr/tfrecords22"]
	]

with g.as_default():
    # creating a model variable on task 0. This is a process running on node vm-8-1
    with tf.device("/job:worker/task:0"):
        w = tf.Variable(tf.random_uniform([num_features]), name="model")
	# read in test set data
	filename_queue = tf.train.string_input_producer(input_data[5], num_epochs=None)
	reader = tf.TFRecordReader()
	_, serialized_example = reader.read(filename_queue)
        features = tf.parse_single_example(serialized_example,
                                           features={'label': tf.FixedLenFeature([1], dtype=tf.int64),
                                                     'index': tf.VarLenFeature(dtype=tf.int64),
                                                     'value': tf.VarLenFeature(dtype=tf.float32)})
        label_test = features['label']
        index_test = features['index']
        value_test = features['value']

        #sparse_params = tf.gather(w, index.values)
        test_x = value_test.values
        test_y = tf.cast(label_test, tf.float32)[0]

        #predict_confidence = tf.reduce_sum(tf.mul(sparse_params, test_x))
        test_a = tf.mul(tf.gather(w, index_test.values), test_x)
        #test_a = tf.mul(w, test_x)
        #test_b = tf.sigmoid(tf.reduce_sum(test_a))
        test_b = tf.reduce_sum(test_a)

        logits = tf.sign(test_b)
        cnt = tf.equal(test_y, logits)
        #norm = tf.reduce_sum(tf.mul(sparse_params, sparse_params))

    # read data
    gradients = []
    for i in range(0, num_works):
        with tf.device("/job:worker/task:%d" % i):
	    # We first define a filename queue for worker i
	    filename_queue = tf.train.string_input_producer(input_data[i], num_epochs=None)

	    # TFRecordReader creates an operator in the graph that reads data from queue
	    reader = tf.TFRecordReader()

	    # Include a read operator with the filenae queue to use. The output is a string
	    # Tensor called serialized_example
	    _, serialized_example = reader.read(filename_queue)


	    # The string tensors is essentially a Protobuf serialized string. With the
	    # following fields: label, index, value. We provide the protobuf fields we are
	    # interested in to parse the data. Note, feature here is a dict of tensors
	    features = tf.parse_single_example(serialized_example,
					       features={
						'label': tf.FixedLenFeature([1], dtype=tf.int64),
						'index' : tf.VarLenFeature(dtype=tf.int64),
						'value' : tf.VarLenFeature(dtype=tf.float32),
					       }
					      )

	    label = features['label']
	    index = features['index']
	    value = features['value']

	    # calculate the gradients
	    y = tf.cast(label, tf.float32)[0]
	    x = value.values
	    tmp_a = tf.mul(tf.gather(w, index.values), x)
	    tmp_b = tf.sigmoid(tf.mul(y, tf.reduce_sum(tmp_a)))
	    tmp_c = tf.mul(y , (tmp_b - 1))
	    local_gradient = tf.mul(tmp_c, x) 	    

	    #local_gradient = tf.ones([num_features, 1], name="operator_%d" % i)
	    gradients.append([index.values, tf.mul(local_gradient, eta)])
   
    # update the model
    with tf.device("/job:worker/task:0"):
	for gradient in gradients:
	    w = tf.scatter_sub(w, gradient[0], gradient[1])
	    #w = tf.subtract(w, gradients[i])
	#aggregator = tf.add_n(gradients)
	
	#assign_op = w.assign_sub(aggregator)
	#assign_op = w.assign_sub(tf.reshape(aggregator, [num_features, 1]))

    '''
    # read test data and calculate the error
    with tf.device("/job:worker/task:0"):
	# We first define a filename queue for worker i
	filename_queue = tf.train.string_input_producer(input_data[5], num_epochs=None)

	# TFRecordReader creates an operator in the graph that reads data from queue
	reader = tf.TFRecordReader()

	# Include a read operator with the filenae queue to use. The output is a string
	# Tensor called serialized_example
	_, serialized_example = reader.read(filename_queue)


	# The string tensors is essentially a Protobuf serialized string. With the
	# following fields: label, index, value. We provide the protobuf fields we are
	# interested in to parse the data. Note, feature here is a dict of tensors
	features = tf.parse_single_example(serialized_example,
				       features={
					'label': tf.FixedLenFeature([1], dtype=tf.int64),
					'index' : tf.VarLenFeature(dtype=tf.int64),
					'value' : tf.VarLenFeature(dtype=tf.float32),
				       }
				      )
	label = features['label']
	index = features['index']
	value = features['value']
    '''	

    with tf.Session("grpc://vm-8-1:2222") as sess:
	print 'Start Initializing Variables'
        sess.run(tf.initialize_all_variables())
	coord = tf.train.Coordinator()
	tf.train.start_queue_runners(sess=sess, coord=coord)
	print 'Training Process Start'
        for i in range(0, num_train_iters):
	    iteration_start_time = time.time()
            output = sess.run(w)
	    iter_duration = time.time()-iteration_start_time
	    print("Step: {}, Time Cost: {}".format(i, iter_duration))
	    #print len(gradients)
	    if i % eval_freq == 0:
		_correct = 0
		for j in range(num_test_iters):
		    _correct += sess.run(cnt)
	    	print("Number of Correct Prediction in Iteration {} is {}/{}".format(i, _correct,num_test_iters))
        sess.close()
