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
from util.util_pickle import *


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
        self.task_queue = Queue()
        self.debug = debug
        self.streaming_data = {}
        self.event_queue = Queue()
        self.task_node_table = None

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

    def runPartition(self, task):
        job_id = task.job_id
        task_id = task.task_id
        # create job if not exist
        if not self.all_task_list.has_key(job_id):
            task_list = {}
            task_list[task_id] = {"status": Status.START,
                                  "data": None
                                  }
            self.all_task_list[job_id] = task_list
        else:
            self.all_task_list[job_id][task_id] = {"status": Status.START,
                                                   "data": None
                                                   }
        debug_print(
            "[Worker]Start task with job : %s task: %s at %s" % (job_id, task_id, time.asctime(time.localtime(time.time()))),
            self.debug)
        result = task.last_rdd.get(task.input_source)
        debug_print("[Worker] Result of Task {0} is generated:{1}".format(task.task_id, result),self.debug)
        self.all_task_list[job_id][task_id] = {"status": Status.FINISH,
                                               "data": result
                                               }
        debug_print(
            "[Worker]Finish task with job : %s task: %s at %s" % (job_id, task_id, time.asctime(time.localtime(time.time()))),
            self.debug)

    def get_rdd_result(self, job_id, task_id, partition_id):
        data = None
        if self.all_task_list.has_key(job_id) and self.all_task_list[job_id].has_key(task_id):
            data = self.all_task_list[job_id][task_id]['data']
            debug_print(
            "[Worker]Get RDD result val {0} with job : {1} task: {2} partition: {3} at {4}".format(data, job_id, task_id, partition_id, time.asctime(time.localtime(time.time()))),
            self.debug)
            #print "%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%data={0}, partition_id={1} isDict={2}".format(data, partition_id, isinstance(data, dict))
            if isinstance(data, dict):
                if data.has_key(int(partition_id)):
                    return data[int(partition_id)]
                else:
                    return []
        return data

    def register(self):
        while self.id is None:
            client = get_client(self.master_address)
            self.id = execute_command(client, client.registerWorker, self.worker_address)
            # self.id = client.registerWorker(self.worker_address)
            if self.id is not None:
                debug_print("worker %d  %s registered at %s " % (
                self.id, self.worker_address, time.asctime(time.localtime(time.time()))), self.debug)
                break
            else:
                gevent.sleep(2)

    def start_task(self, serialized_task, task_node_table):
        task = unpickle_object(serialized_task)
        debug_print("[Worker] Received Task {0}".format(task.task_id), self.debug)
        # event = {
        #     'type' : 'Update',
        #     'data' : task_node_table
        # }
        self.event_queue.put(task_node_table)
        self.task_queue.put(task)
        return 0

    def task_manager(self):
        while True:
            while not self.task_queue.empty():
                task = self.task_queue.get()
                print "Create thread: %s at %s" % (0, time.asctime(time.localtime(time.time())))
                thread = gevent.spawn(self.runPartition, task)
                debug_print("Task created: Key: {0} at {1}".format(
                    task.task_id, time.asctime(time.localtime(time.time()))), self.debug)
            gevent.sleep(0)

    def update_task_node_table(self, task_node_table):
        try:
            self.event_queue.put(task_node_table)
        except:
            return 1
        return 0

    def event_handler(self):
        while True:
            while not self.event_queue.empty():
                task_node_table = self.event_queue.get()
                #update task_node_table
                if self.task_node_table is None :
                    self.task_node_table = task_node_table
                else :
                    for job_task_id, worker_info in  task_node_table :
                        self.task_node_table[job_task_id] = worker_info
            gevent.sleep(0)

    def heartbeat(self):
        while True:
            if self.id is not None:
                # traverse task list and report processing tasks
                task_status_list = {}
                for job_id, task_list in self.all_task_list.items():
                    task_status_list[job_id] = {}
                    for task_id, value in task_list.items():
                        if value['status'] != Status.FINISH_REPORTED:
                            task_status_list[job_id][task_id] = value['status']

                client = get_client(self.master_address)
                debug_print("[Worker]Worker update task status: worker_id: %s at %s" % (
                    self.id, time.asctime(time.localtime(time.time()))), self.debug)
                ret = execute_command(client, client.updateWorkerStatus, self.id, task_status_list)
                # ret = client.updateWorkerStatus(self.id, task_status_list)
                if ret is not None:
                    # client.close()
                    if ret == 0:
                        # if already reported finish task, don't need to report finish again
                        for job_id, task_list in self.all_task_list.items():
                            for task_id, value in task_list.items():
                                if value['status'] == Status.FINISH:
                                    value['status'] = Status.FINISH_REPORTED
            gevent.sleep(2)

    def run(self):
        self.register()
        # self.startRPCServer()
        thread1 = gevent.spawn(self.heartbeat)
        thread2 = gevent.spawn(self.task_manager)
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
