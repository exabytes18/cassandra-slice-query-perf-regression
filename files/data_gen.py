#!/usr/bin/env python

import multiprocessing
import Queue
import random
import sys

from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy, TokenAwarePolicy
from cassandra.query import BatchStatement, BatchType

NUM_PROCESSES = 8
MAX_BATCH_SIZE = 20


def insert_rows(starting_partition, ending_partition, rows_per_partition, counter, counter_lock):
    cluster = Cluster(['127.0.0.1'], load_balancing_policy=TokenAwarePolicy(RoundRobinPolicy()))
    try:
        session = cluster.connect('ks')
        try:
            statement = session.prepare('INSERT INTO tbl (a, b, c, d) VALUES (?, ?, ?, ?)')
            for partition_key in xrange(starting_partition, ending_partition):
                batch = None
                batch_size = 0
                for cluster_column in xrange(rows_per_partition):
                    if batch is None:
                        batch = BatchStatement(batch_type=BatchType.UNLOGGED)
                    value1 = random.randint(1, 1000000)
                    value2 = random.randint(1, 1000000)
                    batch.add(statement, [partition_key, cluster_column, value1, value2])
                    batch_size += 1
                    if (batch_size == MAX_BATCH_SIZE) or (cluster_column + 1 == rows_per_partition):
                        with counter_lock:
                            counter.value += batch_size
                        session.execute(batch)
                        batch = None
                        batch_size = 0
        finally:
            session.shutdown()
    finally:
        cluster.shutdown()


def reporter(counter, counter_lock, total_rows, shutdown_queue):
    while True:
        try:
            shutdown_queue.get(block=True, timeout=1)
            return
        except Queue.Empty:
            pass

        with counter_lock:
            percent = float(counter.value) / total_rows * 100 
            print 'Inserted %s rows (%.2f%%)' % (counter.value, percent)
            sys.stdout.flush()


def insert(num_partitions, rows_per_partition):
    processes = []
    counter = multiprocessing.Value('i', 0)
    counter_lock = multiprocessing.Lock()
    total_rows = num_partitions * rows_per_partition

    shutdown_queue = multiprocessing.Queue()
    reporter_process = multiprocessing.Process(target=reporter, args=(counter, counter_lock, total_rows, shutdown_queue))
    reporter_process.start()

    for process_num in xrange(NUM_PROCESSES):
        starting_partition = int(process_num * num_partitions / NUM_PROCESSES)
        ending_partition = int((process_num + 1) * num_partitions / NUM_PROCESSES)
        process = multiprocessing.Process(target=insert_rows, args=(starting_partition, ending_partition, rows_per_partition, counter, counter_lock))
        process.start()
        processes.append(process)

    for process in processes:
        process.join()

    shutdown_queue.put("done")
    reporter_process.join()
    print '%s' % counter.value
    print 'done'


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print 'usage: %s num_partitions rows_per_partition' % sys.argv[0]
        sys.exit(1)

    num_partitions = int(sys.argv[1])
    rows_per_partition = int(sys.argv[2])
    insert(num_partitions, rows_per_partition)
