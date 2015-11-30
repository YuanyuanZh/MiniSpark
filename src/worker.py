import zerorpc
import gevent
import socket
import StringIO
import pickle
import sys
import time
from gevent.queue import Queue
from util.util_debug import *
from util.util_enum import *
from util.util_zerorpc import *


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
        self.TaskQueue = Queue()
        self.debug = debug
        self.streaming_data = {}

    def getMyAddress(self):
        try:
            csock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            csock.connect(('8.8.8.8', 80))
            (addr, port) = csock.getsockname()
            csock.close()
            return addr + ":" + port
        except socket.error:
            return "127.0.0.1"

    def get_streaming_message(self, value):
       """
       Function to get and store streaming message.

       :param value: spark streaming message
              spark streaming message is "job_id,partition_id,value"
       """
       value_array = value.split(",")
       job_id = value_array[0]
       partition_id = value_array[1]
       value = value_array[2]

       if job_id not in self.streaming_data.keys():
           self.streaming_data[job_id] = {}
       if partition_id not in self.streaming_data[job_id].keys():
           self.streaming_data[job_id][partition_id] = []
       self.streaming_data[job_id][partition_id].append(value)
       self.streaming_data = {}

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

    def runPartition(self, taskStr):
        input = StringIO.StringIO(taskStr)
        unpickler = pickle.Unpickler(input)
        task = unpickler.load()
        key = task.task_id
        # key = partition.rdd_id+":"+partition.partition_id
        self.all_task_list[key] = {"status": Status.START,
                                   "data": None
                                   }
        result = task.last_rdd.get(task.input_source)
        self.all_task_list[key] = {"status": Status.FINISH,
                                   "data": result
                                   }

    def get_rdd_result(self, task_id, partition_id):
        if self.all_task_list.has_key(task_id):
            data =  self.all_task_list[task_id]['data']
            if data is not None:
                if partition_id is None :
                    return data
                else :
                    if data.has_key(partition_id):
                        return data[partition_id]
        return None

    def register(self):
        while self.id is None :
            client = get_client(self.master_address)
            self.id = execute_command(client, client.registerWorker,self.worker_address)
            # self.id = client.registerWorker(self.worker_address)
            if self.id is not None:
                debug_print("worker %d  %s registered at %s " % (self.id, self.worker_address, time.asctime(time.localtime(time.time()))), self.debug)
                break
            else :
                gevent.sleep(2)


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
            if self.id is not None:
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
                            for key, value in self.all_task_list.items():
                                if value['status'] == Status.FINISH:
                                    value['status'] = Status.FINISH_REPORTED
                except  zerorpc.LostRemote:
                    print "RPC error: lost remote"
                    pass
            gevent.sleep(2)

    def run(self):
        self.register()
        # self.startRPCServer()
        thread1 = gevent.spawn(self.heartbeat)
        thread2 = gevent.spawn(self.TaskManager())
        thread3 = gevent.spawn(self.startRPCServer)
        # self.startRPCServer()
        gevent.joinall([thread1, thread3, thread2])


if __name__ == '__main__':
    master_address = sys.argv[1]
    worker_address = sys.argv[2]
    if len(sys.argv) == 4:
        if sys.argv[3] == 'debug':
            debug = True
    elif len(sys.argv) == 3:
        debug = False
    worker = Worker(master_address, worker_address, debug)
    worker.run()
