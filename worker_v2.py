from multiprocessing.managers import BaseManager
from multiprocessing import Queue, Pool, cpu_count
import sys
import time
from argparse import ArgumentParser


class QueueManager(BaseManager):
    pass


def connect(ip, port):
    QueueManager.register('get_input_queue')
    QueueManager.register('get_res_queue')
    manager = QueueManager(address=(ip, int(port)), authkey='abcd654'.encode())
    manager.connect()
    queue1 = manager.get_input_queue()
    queue2 = manager.get_res_queue()
    
    return queue1, queue2


def multiply(A, X, range_):
    a, b = range_
    results = []
    for tup in A[a:b]:
        idx, A = tup
        ncols = len(A)
        res = 0
        for i in range(ncols):
            res += A[i] * X[i][0]
        
        results.append((idx, res))
    return results


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('ip', type=str)
    parser.add_argument('port', type=str)
    parser.add_argument('-s', '--subprocesses', type=int, default=cpu_count()-1)
    args = parser.parse_args()

    input_queue, res_queue = connect(args.ip, args.port)

    while True:
        print("Waiting for tasks...")
        while True:
            if input_queue.empty():
                time.sleep(2)
            else:
                print("Tasks received...")
                break

        X = input_queue.get()
        n_tasks = input_queue.get()
        n_proc = args.subprocesses + 1
        print(f'Subprocesses: {args.subprocesses}')
        tasks_per_subprocess = n_tasks // n_proc
        main_proc_tasks = tasks_per_subprocess + n_tasks % n_proc

        tasks_received = 0
        tasks = []
        while tasks_received != n_tasks:
            if not input_queue.empty():
                tasks.append(input_queue.get())
                tasks_received += 1

        start_time = time.time()
        pool = Pool(args.subprocesses)

        results = []
        for i in range(1, args.subprocesses+1): 
            results.append(pool.apply_async(multiply, args=(tasks, X, 
                (main_proc_tasks + (i-1)*tasks_per_subprocess, main_proc_tasks + i*tasks_per_subprocess)))
            )

        pool.close()

        for res in multiply(tasks, X, (0, main_proc_tasks)):
            res_queue.put(res)

        pool.join()
        
        for res in results:
            tups = res.get()
            for t in tups:
                res_queue.put(t)

        print('Calculations finished')
        print(f'Time: {time.time() - start_time} sec // {n_proc} processes // {n_tasks} tasks')