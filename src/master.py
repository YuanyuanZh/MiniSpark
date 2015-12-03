import zerorpc
import sys
import gevent
from gevent.queue import Queue

import random
from gevent.lock import *
import time
import os
from src.driver import SparkDriver

from util.util_enum import *
from util.util_debug import *
from util.util_pickle import *
from util.util_zerorpc import *


class Master():
    def __init__(self, port, debug):
        self.port = port
        self.worker_list = {}
        self.worker_id = -1
        self.worker_status_list = {}
        self.event_queue = Queue()
        self.debug = debug
        self.job_id = 0
        self.job_list = {}
        self.driver_list = {}


    def registerWorker(self, worker_address):
        self.worker_id += 1
        worker = {
            "worker_id": self.worker_id,
            "address": worker_address,
            'num_slots': 5
        }
        # self.worker_list[self.worker_id] = worker
        # self.reportEvent('REGISTER_WORKER', worker)
        # self.worker_status_list[self.worker_id] = WorkerStatus(self.worker_id, worker_address, "RUNNING", None, None)
        self.worker_list[worker['worker_id']] = worker
        debug_print( "Worker %s %s registered at %s" % (
            worker['worker_id'], worker['address'], time.asctime(time.localtime(time.time()))), self.debug)
        return self.worker_id

    def get_available_worker(self):
        for worker_id, worker in self.worker_list.items():
            if worker['num_slots'] > 0 :
                return worker
        return None

    def get_worker_list(self):
        return self.worker_list

    def assign_task(self, task, worker_id):
        task_str = pickle_object(task)
        worker_address = self.worker_list[worker_id]['address']
        client = get_client(worker_address)
        ret = execute_command(client,client.start_task,task_str)
        if ret == 0:
            self.worker_list[worker_id]['num_slots'] -= 1
            debug_print("Assign task successfully: worker_id: %s job: %s task: %s at %s" % (
                            worker_id, task.job_id, task.task_id, time.asctime(time.localtime(time.time()))), self.debug)

    def get_rdd_result(self, task, partition_id):
        worker_address = task.worker['address']
        job_id = task.job_id
        task_id = task.task_id
        client = get_client(worker_address)
        data = execute_command(client, client.get_rdd_result, job_id, task_id, partition_id)
        debug_print("Get RDD result task: job: %s task: %s at %s" % (
                            job_id, task_id, time.asctime(time.localtime(time.time()))), self.debug)

        return data

    def heartBeat(self):
        # print "enter heartbeat : at %s" %time.asctime( time.localtime(time.time()) )
        while True:
            for worker_id in self.worker_status_list.keys():
                #
                status = self.worker_status_list[worker_id]
                if status['num_heartbeat'] == status['num_callback']:
                    status['timeout_times'] += 1
                    if status['timeout_times'] == 3:
                        status['worker_status'] = Worker_Status.DOWN
                        if self.worker_list.has_key(worker_id) :
                            if self.worker_list[worker_id] is not None :
                                del self.worker_list[worker_id]
                                # self.worker_list[worker_id]['num_slots'] == 0
                        for job_id in self.driver_list.keys():
                            self.driver_list[job_id][0].fault_handler(worker_id)
                        # self.reportEvent(Event.WORKER_DOWN, worker_id)
                        debug_print("Report Worker Down: worker_id: %s at %s" % (
                            worker_id, time.asctime(time.localtime(time.time()))), self.debug)
                        # print "Find worker down: worker_id: %s, ip: %s at %s" % (
                        #     workerStatus.worker_id, workerStatus.worker_address,
                        #     time.asctime(time.localtime(time.time())))
                else:
                    status['num_heartbeat'] = status['num_callback']
                    status['timeout_times'] = 0
            gevent.sleep(2)

    def reportEvent(self, type, status):
        event = {'type' : type,
                 'status': status
                 }

        # self.event_queue.put_nowait(event)


    def updateWorkerStatus(self, worker_id, task_status_list):
        # update worker status
        if not self.worker_status_list.has_key(worker_id):
            # create this worker status if not exist
            status = {
                'worker_status': Worker_Status.UP,
                'num_callback': 0,
                'timeout_times': 0,
                'num_heartbeat': 0,
                'task_status_list': task_status_list
            }
            self.worker_status_list[worker_id] = status
        else:
            origin_status = self.worker_status_list[worker_id]
            if origin_status['worker_status'] != 'Down':
                origin_status['task_status_list'] = task_status_list
                origin_status['num_callback'] = random.random()
        # check task status
        for job_id, task_list in task_status_list.items():
            for task_id, status in task_list.items():
                if status == Status.FINISH :
                    if self.worker_list[worker_id] is not None:
                        self.worker_list[worker_id]['num_slots'] += 1
                    driver = self.find_driver(job_id)
                    if driver is not None:
                        driver.finish_task(task_id)
                        # self.reportEvent(Event.FINISH_TASK, key)
                        debug_print("Report Task Finish: worker_id: %s job_id %s task_id: %s at %s" % (
                        worker_id, job_id, task_id, time.asctime(time.localtime(time.time()))), self.debug)
        return 0

    def find_driver(self,job_id):
        if self.driver_list.has_key(job_id):
            return self.driver_list[job_id][0]
        else:
            return None

    def run(self):
        thread1 = gevent.spawn(self.heartBeat)
        debug_print("Heartbeat started at %s" %(time.asctime( time.localtime(time.time()))),self.debug)
        thread2 = gevent.spawn(self.rpcServer)
        debug_print("RPC server started at %s" %(time.asctime( time.localtime(time.time()))), self.debug)
        gevent.joinall([thread1,thread2])
        # gevent.joinall([gevent.spawn(self.jobScheduler()), gevent.spawn(self.heartBeat()), gevent.spawn(self.collectJobResult())])

    def rpcServer(self):
        # print "enter rpc"
        rpc_server = zerorpc.Server(self)
        addr = "tcp://0.0.0.0:" + self.port
        # print "address: %s", addr
        rpc_server.bind(addr)
        # print "rpc run 1"
        rpc_server.run()
        # print "rpc run"

    def get_job(self, job, client_address):
        #TODO make a dict {job_id: client_info} and Gevent
        try:
            job_id = self.job_id
            driver = SparkDriver(job_id)
            self.job_list[job_id] = unpickle_object(job)
            self.job_list[job_id].run(driver)
            self.driver_list[job_id] = (driver,client_address)
            self.job_id += 1
        except Exception as e:
            debug_print("Create job: %s from client: %s failed at %s" % (
            self.job_id, client_address, time.asctime(time.localtime(time.time()))), self.debug)
            return -1
        return self.job_id

    def return_client(self, job_id, result):
        if self.driver_list.has_key(job_id):
            client_address = self.driver_list[job_id][1]
            client = get_client(client_address)
            debug_print("Finish job: %s for client %s at %s" % (
            job_id, client_address, time.asctime(time.localtime(time.time()))), self.debug)
            execute_command(client, client._print_message, 'Finish job with result: ' + result)


    # def produce_new_driver(self, job_id):
    #     return SparkDriver()

if __name__ == '__main__':
    status = Worker_Status.UP
    port = sys.argv[1]
    if len(sys.argv) == 3:
        if sys.argv[2] == 'debug':
            debug = True
    elif len(sys.argv) == 2:
        debug = False
    master = Master(port, debug)
    #SparkDriver._master = master
    master.run()
    # rpc_server = zerorpc.Server(master)
    # addr = "tcp://0.0.0.0:" + port
    # rpc_server.bind(addr)
    # print "rpc run 1"
    # rpc_server.run()
    # print "rpc run"


