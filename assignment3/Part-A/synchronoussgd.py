import tensorflow as tf
import os
import time
import datetime

worker_num = 5
num_features = 33762578
# iteration num, should be 2e7
iterate_num = int(2e4)
# test num is test size, should be 1e4
testset_size = int(1e4)
# break point is how often we run a test, should be 1e5
break_point = int(2e3)
lr = 0.01

g = tf.Graph()

input_producers = [
        ["/home/ubuntu/criteo-tfr/tfrecords00", "/home/ubuntu/criteo-tfr/tfrecords01", "/home/ubuntu/criteo-tfr/tfrecords02", "/home/ubuntu/criteo-tfr/tfrecords03", "/home/ubuntu/criteo-tfr/tfrecords04"],
        ["/home/ubuntu/criteo-tfr/tfrecords05", "/home/ubuntu/criteo-tfr/tfrecords06", "/home/ubuntu/criteo-tfr/tfrecords07", "/home/ubuntu/criteo-tfr/tfrecords08", "/home/ubuntu/criteo-tfr/tfrecords09"],
        ["/home/ubuntu/criteo-tfr/tfrecords10", "/home/ubuntu/criteo-tfr/tfrecords11", "/home/ubuntu/criteo-tfr/tfrecords12", "/home/ubuntu/criteo-tfr/tfrecords13", "/home/ubuntu/criteo-tfr/tfrecords14"],
        ["/home/ubuntu/criteo-tfr/tfrecords15", "/home/ubuntu/criteo-tfr/tfrecords16", "/home/ubuntu/criteo-tfr/tfrecords17", "/home/ubuntu/criteo-tfr/tfrecords18", "/home/ubuntu/criteo-tfr/tfrecords19"],
        ["/home/ubuntu/criteo-tfr/tfrecords20", "/home/ubuntu/criteo-tfr/tfrecords21"],
        ["/home/ubuntu/criteo-tfr/tfrecords22"]]


with g.as_default():
    with tf.device("/job:worker/task:0"):
        w = tf.Variable(tf.random_normal([num_features]), name="model")

    # 1. update indice to parameter server
    value_list = [0, 0, 0, 0, 0]
    index_list = [0, 0, 0, 0, 0]
    label_list = [0, 0, 0, 0, 0]
    for i in range(worker_num):
        with tf.device("/job:worker/task:%d" % i):
	    filename_queue = tf.train.string_input_producer(input_producers[i], num_epochs=None)
            reader = tf.TFRecordReader()
            _, serialized_example = reader.read(filename_queue)
            features = tf.parse_single_example(serialized_example,
                                               features={'label': tf.FixedLenFeature([1], dtype=tf.int64),
                                                         'index': tf.VarLenFeature(dtype=tf.int64),
                                                         'value': tf.VarLenFeature(dtype=tf.float32)})
            label = features['label']
            index = features['index']
            value = features['value']
            label_list[i] = label
            index_list[i] = index
            value_list[i] = value

    # 2. push dense weights to all workers
    weight_list = [0, 0, 0, 0, 0]
    with tf.device("/job:worker/task:0"):
        for i in range(worker_num):
            index = index_list[i]
            weight_list[i] = tf.gather(w, index.values)

    # 3. calculate gradients
    gradients = [0, 0, 0, 0, 0]
    output = []
    for i in range(worker_num):
        with tf.device("/job:worker/task:%d" % i):
            weight = weight_list[i]
            value = value_list[i]

            y = tf.cast(label_list[i], tf.float32)[0]
            x = value.values
            a = tf.mul(weight, x)

            b = tf.reduce_sum(a)
            c = tf.sigmoid(tf.mul(y, b))
            d = tf.mul(y, c-1)
            output.append(d)
            local_gradient = tf.mul(d, x)
            gradients[i] = tf.mul(local_gradient, lr)

    # 4. update gradients
    with tf.device("/job:worker/task:0"):
        tmp1 = gradients
        tmp2 = index_list
        tmp3 = output

        w = tf.scatter_sub(w, index_list[0].values, gradients[0])
        w = tf.scatter_sub(w, index_list[1].values, gradients[1])
        w = tf.scatter_sub(w, index_list[2].values, gradients[2])
        w = tf.scatter_sub(w, index_list[3].values, gradients[3])
        w = tf.scatter_sub(w, index_list[4].values, gradients[4])

    # 5. calculate test error
    with tf.device("/job:worker/task:0"):
        filename_queue = tf.train.string_input_producer(input_producers[5], num_epochs=None)
        reader = tf.TFRecordReader()
        _, serialized_example = reader.read(filename_queue)
        features = tf.parse_single_example(serialized_example,
                                           features={'label': tf.FixedLenFeature([1], dtype=tf.int64),
                                                     'index': tf.VarLenFeature(dtype=tf.int64),
                                                     'value': tf.VarLenFeature(dtype=tf.float32)})
        label = features['label']
        index = features['index']
        value = features['value']

        sparse_params = tf.gather(w, index.values)
        test_x = value.values
        test_y = tf.cast(label, tf.float32)[0]

        predict_confidence = tf.reduce_sum(tf.mul(sparse_params, test_x))
        predict_y = tf.sign(predict_confidence)
        cnt = tf.equal(test_y, predict_y)
        norm = tf.reduce_sum(tf.mul(sparse_params, sparse_params))

    with tf.Session("grpc://vm-8-1:2222") as sess:
        print("Program start at: {}".format(datetime.datetime.now()))
        sess.run(tf.initialize_all_variables())
        coord = tf.train.Coordinator()
        threads = tf.train.start_queue_runners(sess=sess, coord=coord)
	_time_counter = 0
        for i in range(1, 1+iterate_num):
	    iter_start_time = time.time()
	    output = sess.run([tmp1, tmp2, tmp3])
	    iter_duration = time.time() - iter_start_time
	    _time_counter += iter_duration
            if i % break_point == 0:
                current_error = 0
		print 'iteration: {}/{}, time cost: {}'.format(i, iterate_num, _time_counter)
		_time_counter = 0
                for _ in range(testset_size):
                    eval_output = sess.run([test_y, predict_y, cnt, norm])
                    correct_num = eval_output[2]
                    if not correct_num:
                        current_error += 1
		print("Number of Err in Step: {} is {}/{}".format(i, current_error, testset_size))
        print("Program ended at: {}".format(datetime.datetime.now()))
        sess.close()
