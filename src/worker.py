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


class Worker():
    def __init__(self, master_address, worker_address=None):
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
        result = partition.get()
        key = partition.rdd_id+":"+partition.partition_id
        self.all_task_list[key] = { }


    def register(self):
        client = zerorpc.Client()
        client.connect('tcp://' + self.master_address)
        self.id = client.registerWorker(self.worker_address)
        if self.id is not None:
            client.close()
            addr = self.worker_address
            debug_print("worker %d  %s registered at %s " % (self.id, addr, time.asctime(time.localtime(time.time()))))


    def startTask(self, task):
        self.TaskQueue.put(task)
        return 0

    def TaskManager(self):
        while True:
            while not self.TaskQueue.empty():
                mapperTask = self.TaskQueue.get()
                # print "Create map thread: %s at %s" % (0, time.asctime(time.localtime(time.time())))
                thread = gevent.spawn(self.mapper, mapperTask)
                print "Mapper created: Key: %d at %s" % (
                        mapperTask.split_id, time.asctime(time.localtime(time.time())))
            gevent.sleep(0)

    def ReducerManage(self):
        while True:
            while not self.ReducerTaskQueue.empty():
                reducerTask = self.ReducerTaskQueue.get()
                # print "Create reduce thread: %s at %s" % (0, time.asctime(time.localtime(time.time())))
                thread = gevent.spawn(self.reducer, reducerTask)
                print "Reduce created: Key: %d at %s" % (
                    reducerTask.partition_id, time.asctime(time.localtime(time.time())))
            gevent.sleep(0)

    def heartbeat(self):
        while True:
            Local_current_mapper = self.current_mapper
            Local_current_Reducer = self.current_reducer
            if self.current_mapper is not None:
                status_mapper = MapperStatus(self.current_mapper.job_id, self.current_mapper.split_id,
                                             self.current_mapper.task_id, self.current_mapper.state,
                                             self.current_mapper.progress, self.current_mapper.changeToFinish)
                status_mapper_dict = status_mapper.__dict__
            else:
                status_mapper = None
                status_mapper_dict = None
            if self.current_reducer is not None:
                status_reducer = ReducerStatus(self.current_reducer.job_id, self.current_reducer.partition_id,
                                               self.current_reducer.task_id, self.current_reducer.state,
                                               self.current_reducer.progress, self.current_reducer.changeToFinish)
                status_reducer_dict = status_reducer.__dict__
            else:
                status_reducer = None
                status_reducer_dict = None
            status = WorkerStatus(self.id, self.worker_address, "RUNNING", status_mapper, status_reducer)
            status_dict = {
                'worker_id': self.id,
                'worker_address': self.worker_address,
                'worker_status': 'RUNNING',
                'num_heartbeat': 0,
                'num_callback': 0,
                'timeout_times': 0,
                'mapper_status': status_mapper_dict,
                'reducer_status': status_reducer_dict
            }
            # print "send status"
            client = zerorpc.Client()
            client.connect('tcp://' + self.master_address)
            # if status.mapper_status is not None:
            # print "Worker update status UPdate: worker_id: %s, mapper key: %s at %s" % (
            #     self.id, status.mapper_status.split_id, time.asctime(time.localtime(time.time())))
            try:
                ret = client.updateWorkerStatus(status_dict)
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
    if len(sys.argv) == 3:
        worker_address = sys.argv[2]
    else:
        worker_address = None
    worker = Worker(master_address, worker_address)
    worker.run()
