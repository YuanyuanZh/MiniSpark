import zerorpc
import gevent
import socket
import StringIO
import pickle
import sys
import time
import json
from gevent.queue import Queue
from util.util_debug import *
from util.util_enum import *


class Worker():
    def __init__(self, master_address, worker_address, debug):
        self.id = None
        self.master_address = master_address
        if (worker_address is None):
            self.worker_address = self.getMyAddress()
            self.is_remote = False
        else:
            self.worker_address = worker_address
            self.is_remote = True
        self.all_task_list = {}
        self.current_mapper = None
        self.current_reducer = None
        self.TaskQueue = Queue()
        self.debug = debug

    def getMyAddress(self):
        try:
            csock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            csock.connect(('8.8.8.8', 80))
            (addr, port) = csock.getsockname()
            csock.close()
            return addr + ":" + port
        except socket.error:
            return "127.0.0.1"

    def startRPCServer(self):
        master = zerorpc.Server(self)
        if self.is_remote:
            addr = self.worker_address
        else:
            addr = "0.0.0.0:" + self.worker_address.split(":")[1]
        # print "worker address is: %s at %s " % (addr, time.asctime(time.localtime(time.time())))
        # addr = "tcp://0.0.0.0:"+port
        master.bind('tcp://' + addr)
        master.run()

    def runPartition(self, partitionStr):
        input = StringIO.StringIO(partitionStr)
        unpickler = pickle.Unpickler(input)
        partition = unpickler.load()
        key = partition.rdd_id+":"+partition.partition_id
        self.all_task_list[key] = {"status": Status.START,
                                   "data": None
                                   }
        result = partition.get()
        self.all_task_list[key] = {"status": Status.FINISH,
                                   "data": result
                                   }


    def register(self):
        client = zerorpc.Client()
        client.connect('tcp://' + self.master_address)
        self.id = client.registerWorker(self.worker_address)
        if self.id is not None:
            client.close()
            addr = self.worker_address
            debug_print("worker %d  %s registered at %s " % (self.id, addr, time.asctime(time.localtime(time.time()))), self.debug)


    def startTask(self, task):
        self.TaskQueue.put(task)
        return 0

    def TaskManager(self):
        while True:
            while not self.TaskQueue.empty():
                task = self.TaskQueue.get()
                # print "Create map thread: %s at %s" % (0, time.asctime(time.localtime(time.time())))
                thread = gevent.spawn(self.runPartition, task)
                debug_print("Task created: Key: %d at %s" % (
                        task.partition_id, time.asctime(time.localtime(time.time()))), self.debug)
                print
            gevent.sleep(0)

    def heartbeat(self):
        while True:
            #traverse task list and report processing tasks
            task_status_list = {}
            for key, value in self.all_task_list.items():
                if value['status'] != Status.FINISH_REPORTED:
                    task_status_list[key] = value['status']
            # print "send status"
            client = zerorpc.Client()
            client.connect('tcp://' + self.master_address)
            # if status.mapper_status is not None:
            # print "Worker update status UPdate: worker_id: %s, mapper key: %s at %s" % (
            #     self.id, status.mapper_status.split_id, time.asctime(time.localtime(time.time())))
            try:
                debug_print("Worker update task status: worker_id: %s at %s" % (
                    self.id, time.asctime(time.localtime(time.time()))), self.debug)
                ret = client.updateWorkerStatus(self.id, task_status_list)
                if ret is not None:
                    client.close()
                    if ret == 0:
                        if Local_current_mapper is not None:
                            if Local_current_mapper.changeToFinish == True:
                                # print "Worker update status Suc: worker_id: %s, key: %d C_key: %d at %s" % (
                                # self.id, status.mapper_status.split_id, self.current_mapper.split_id,time.asctime(time.localtime(time.time())))
                                Local_current_mapper.changeToFinish = False
                        if Local_current_Reducer is not None:
                            if Local_current_Reducer.changeToFinish == True:
                                Local_current_Reducer.changeToFinish = False
                    else:
                        print "Worker update status failed with undefined return: worker_id: %s, key: at %s" % (
                        self.id, time.asctime(time.localtime(time.time())))
                else:
                    print "Worker update status failed: worker_id: %s, key: at %s" % (
                        self.id, time.asctime(time.localtime(time.time())))
            except  zerorpc.LostRemote:
                print "RPC error: lost remote"
                pass
            gevent.sleep(2)

    def run(self):
        self.register()
        # self.startRPCServer()
        thread1 = gevent.spawn(self.heartbeat)
        thread2 = gevent.spawn(self.MapperManage)
        thread3 = gevent.spawn(self.ReducerManage)
        thread4 = gevent.spawn(self.startRPCServer)
        # self.startRPCServer()
        gevent.joinall([thread1, thread3, thread2, thread4])


if __name__ == '__main__':
    master_address = sys.argv[1]
    worker_address = sys.argv[2]
    if len(sys.argv) == 4:
        debug = sys.argv[3]
    elif len(sys.argv) == 3:
        debug = True
    worker = Worker(master_address, worker_address, debug)
    worker.run()
