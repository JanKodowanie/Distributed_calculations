from multiprocessing.managers import BaseManager
from multiprocessing import Queue
import sys


class QueueManager(BaseManager):
    pass


def main(ip, port):
    input_queue = Queue()
    res_queue = Queue()
    QueueManager.register('get_input_queue', callable=lambda:input_queue)
    QueueManager.register('get_res_queue', callable=lambda:res_queue)
    manager = QueueManager(address=(ip, int(port)), authkey='abcd654'.encode())
    server = manager.get_server()
    print("Kolejki zostały stworzone i udostępnione")
    server.serve_forever()


if __name__ == '__main__':
    main(*sys.argv[1:])
