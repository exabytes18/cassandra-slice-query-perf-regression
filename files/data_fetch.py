#!/usr/bin/env python

import multiprocessing
import Queue
import random
import sys
import time

from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy, TokenAwarePolicy
from cassandra.query import BatchStatement, BatchType

QUERIES_PER_ITERATION = 10


def fetch_rows(num_partitions, num_queries, counter, counter_lock):
    cluster = Cluster(['127.0.0.1'], load_balancing_policy=TokenAwarePolicy(RoundRobinPolicy()))
    try:
        session = cluster.connect('ks')
        try:
            statement = session.prepare('SELECT * FROM tbl WHERE a = ?')
            while True:
                with counter_lock:
                    iteration_size = min(QUERIES_PER_ITERATION, num_queries - counter.value)
                    if iteration_size > 0:
                        counter.value += iteration_size
                    else:
                        break

                for x in xrange(iteration_size):
                    random_partition_key = random.randint(0, num_partitions)
                    for row in session.execute(statement, [random_partition_key]):
                        pass
        finally:
            session.shutdown()
    finally:
        cluster.shutdown()


def reporter(counter, counter_lock, num_queries, shutdown_queue):
    while True:
        try:
            shutdown_queue.get(block=True, timeout=1)
            return
        except Queue.Empty:
            pass

        with counter_lock:
            percent = float(counter.value) / num_queries * 100 
            print 'Executed %s queries (%.2f%%)' % (counter.value, percent)
            sys.stdout.flush()


def fetch(num_processes, num_partitions, num_queries):
    counter = multiprocessing.Value('i', 0)
    counter_lock = multiprocessing.Lock()

    shutdown_queue = multiprocessing.Queue()
    reporter_process = multiprocessing.Process(target=reporter, args=(counter, counter_lock, num_queries, shutdown_queue))
    reporter_process.start()

    processes = []
    start_time = time.time()
    for process_num in xrange(num_processes):
        process = multiprocessing.Process(target=fetch_rows, args=(num_partitions, num_queries, counter, counter_lock))
        process.start()
        processes.append(process)

    for process in processes:
        process.join()
    end_time = time.time()

    shutdown_queue.put("done")
    reporter_process.join()

    total_time = end_time - start_time
    print '%s queries in %.3f seconds (%.3f qps)' % (counter.value, total_time, counter.value / total_time)
    print 'done'


if __name__ == '__main__':
    if len(sys.argv) != 4:
        print 'usage: %s num_processes num_partitions num_queries' % sys.argv[0]
        sys.exit(1)

    num_processes = int(sys.argv[1])
    num_partitions = int(sys.argv[2])
    num_queries = int(sys.argv[3])
    fetch(num_processes, num_partitions, num_queries)
