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


def multiply(X, n_tasks, ip, port):
    input_queue, res_queue = connect(ip, port)
    done = 0
    while done != n_tasks:
        if not input_queue.empty():
            idx, A = input_queue.get()
            ncols = len(A)
            res = 0
            for i in range(ncols):
                res += A[i] * X[i][0]
            
            res_queue.put((idx, res))
            done += 1


def multiply_main(X, n_tasks, input_queue, res_queue):
    done = 0
    while done != n_tasks:
        if not input_queue.empty():
            idx, A = input_queue.get()
            ncols = len(A)
            res = 0
            for i in range(ncols):
                res += A[i] * X[i][0]
            
            res_queue.put((idx, res))
            done += 1


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('ip', type=str)
    parser.add_argument('port', type=str)
    parser.add_argument('-s', '--subprocesses', type=int, default=cpu_count()-1)
    args = parser.parse_args()

    input_queue, res_queue = connect(args.ip, args.port)

    while True:
        print("Czekam na zadania...")
        while True:
            if input_queue.empty():
                time.sleep(2)
            else:
                print("Otrzymalem zadania...")
                break

        X = input_queue.get()
        n_tasks = input_queue.get()
        n_proc = args.subprocesses + 1
        print(f'Liczba podprocesów: {args.subprocesses}')
        tasks_per_subprocess = n_tasks // n_proc
        main_proc_tasks = tasks_per_subprocess + n_tasks % n_proc

        start_time = time.time()
        pool = Pool(args.subprocesses)
        for i in range(args.subprocesses): 
            pool.apply_async(multiply, args=(X, tasks_per_subprocess, args.ip, args.port))

        pool.close()
        multiply_main(X, main_proc_tasks, input_queue, res_queue)
        pool.join()
        
        print('Koniec obliczeń')
        print(f'Czas: {time.time() - start_time} // {n_proc} procesow // {n_tasks} zadan')