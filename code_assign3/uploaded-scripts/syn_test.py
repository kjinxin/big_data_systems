import tensorflow as tf
import os
import datetime

worker_num = 5
num_features = 33762578
# iteration num, should be 2e7
iterate_num = int(10)
# test num is test size, should be 1e4
test_num = int(10)
# break point is how often we run a test, should be 1e5
break_point = int(10)

g = tf.Graph()

input_producers = [
	["./data/criteo-tfr-tiny/tfrecords00", "./data/criteo-tfr-tiny/tfrecords01", "./data/criteo-tfr-tiny/tfrecords02", "./data/criteo-tfr-tiny/tfrecords03", "./data/criteo-tfr-tiny/tfrecords04"],
	["./data/criteo-tfr-tiny/tfrecords05", "./data/criteo-tfr-tiny/tfrecords06", "./data/criteo-tfr-tiny/tfrecords07", "./data/criteo-tfr-tiny/tfrecords08", "./data/criteo-tfr-tiny/tfrecords09"],
	["./data/criteo-tfr-tiny/tfrecords10", "./data/criteo-tfr-tiny/tfrecords11", "./data/criteo-tfr-tiny/tfrecords12", "./data/criteo-tfr-tiny/tfrecords13", "./data/criteo-tfr-tiny/tfrecords14"],
	["./data/criteo-tfr-tiny/tfrecords15", "./data/criteo-tfr-tiny/tfrecords16", "./data/criteo-tfr-tiny/tfrecords17", "./data/criteo-tfr-tiny/tfrecords18", "./data/criteo-tfr-tiny/tfrecords19"],
	["./data/criteo-tfr-tiny/tfrecords20", "./data/criteo-tfr-tiny/tfrecords21"],
	["./data/criteo-tfr-tiny/tfrecords22"]
	]

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
    ooo = []
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
            ooo.append(d)
            local_gradient = tf.mul(d, x)
            gradients[i] = tf.mul(local_gradient, 0.01)

    # 4. update gradients
    with tf.device("/job:worker/task:0"):
        aaa = gradients
        bbb = index_list
        ccc = ooo
	for i in range(worker_num):
        	w = tf.scatter_sub(w, index_list[i].values, tf.reshape(gradients[i], [num_features, 1]))
        #w = tf.scatter_sub(w, index_list[1].values, gradients[1])
        #w = tf.scatter_sub(w, index_list[2].values, gradients[2])
        #w = tf.scatter_sub(w, index_list[3].values, gradients[3])
        #w = tf.scatter_sub(w, index_list[4].values, gradients[4])

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
        print datetime.datetime.now()
        sess.run(tf.initialize_all_variables())
        coord = tf.train.Coordinator()
        threads = tf.train.start_queue_runners(sess=sess, coord=coord)
        for i in range(1, 1+iterate_num):
            if i % 1000 == 0:
                print 'iteration {}/{}'.format(i, iterate_num)
            output = sess.run([aaa, bbb, ccc])
	    print w.eval()
            if i % break_point == 0:
                current_error = 0
                out = open('error_syn.csv', 'a')
                for _ in range(test_num):
                    output2 = sess.run([test_y, predict_y, cnt, norm])
                    is_right = output2[2]
                    if not is_right:
                        current_error += 1
                print >> out, current_error
                out.close()
        print datetime.datetime.now()
        sess.close()