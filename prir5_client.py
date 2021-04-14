from multiprocessing.managers import BaseManager
from multiprocessing import Queue
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


def read(fname):
    f = open(fname, "r")
    nr = int(f.readline())
    nc = int(f.readline())

    A = [[0] * nc for x in range(nr)]
    r = 0
    c = 0
    for i in range(0,nr*nc):
        A[r][c] = float(f.readline())
        c += 1
        if c == nc:
            c = 0
            r += 1
    f.close()
    return A


def save_results(B):
    f = open('res.dat', "w")
    lines = [str(len(B)) + "\n", str(1) + "\n"]
    lines.extend([str(i) + "\n" for i in B])
    f.writelines(lines)
    f.close()


if __name__ == '__main__':
    start_time = time.time()

    parser = ArgumentParser()
    parser.add_argument('ip', type=str)
    parser.add_argument('port', type=str)
    parser.add_argument('-m', '--matrix', type=str, default="A.dat")
    parser.add_argument('-v', '--vector', type=str, default="X.dat")
    parser.add_argument('-t', '--tasks', type=int)   
    args = parser.parse_args()

    input_queue, res_queue = connect(args.ip, args.port)
    A = read(args.matrix)
    X = read(args.vector)
    n = len(A)
    m = len(X)

    tasks = n
    if args.tasks:
        tasks = args.tasks

    if len(A[0]) != m:
        print("Niezgodne wymiary macierzy A i X")
        sys.exit(-1)

    # first -- send X and number of tasks
    input_queue.put(X)
    input_queue.put(tasks)
    for i in range(tasks):
        # then send parts of A
        input_queue.put((i, A[i]))

    B = [0 for i in range(tasks)]
    res_received = 0

    time_slept = 0
    print("Czekam na wyniki...")
    while True:
        if res_queue.empty():
            time_slept += 2
            time.sleep(2)
        else:
            print("Otrzymalem wyniki...")
            break
            
    while not res_received == tasks:
        if not res_queue.empty():
            idx, res = res_queue.get()
            B[idx] = res
            res_received += 1

    save_results(B)
    print(f'Wyniki zapisane do pliku res.dat')
    print(f'Całkowity czas działania programu: {time.time() - start_time - time_slept} sekund')