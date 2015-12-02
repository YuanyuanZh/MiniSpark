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

    def jobScheduler(self):
        # print "enter scheduler : at %s" %time.asctime( time.localtime(time.time()) )
        job = None
        while True:
            while not self.jobs.empty():
                if job is None or job.state == 'COMPLETE':
                    job = self.jobs.get()
                    self.processing_jobs[job.jobId] = job
                    # num_map_tasks = 0
                    # num_reducer_tasks = 0
                    # for i in range(len(self.worker_list)):
                    #     if self.worker_list[i]['mapper'] == None:
                    #         num_map_tasks += 1
                    #     if self.worker_list[i]['reducer'] == None:
                    #         num_reducer_tasks += 1
                    partitions = []

                    # assign tasks in creating job
                    self.assignTask('mapper', job.map_task_list)
                    self.assignTask('reducer', job.reduce_task_list)
                    self.processing_jobs[job.jobId].state = 'PROCESSING'
                    self.processing_jobs[job.jobId].progress = '0%'
                    # while job.state != 'COMPLETE':
            while not self.event_queue.empty():
                event = self.event_queue.get_nowait()
                if event.type == 'MAPPER_FINISHED':
                    workerStatus = event.status
                    # print "Mapper task %s finished. worker_id: %s %s at %s" % (
                    #     workerStatus.mapper_status.split_id,workerStatus.worker_id,
                    #     workerStatus.worker_address, time.asctime(time.localtime(time.time())))
                    self.finishMapper(event.status, job.map_task_list)
                if event.type == 'REDUCER_FINISHED':
                    workerStatus = event.status
                    # print "Reducer task %s finished. worker_id: %s %s at %s" % (
                    #     workerStatus.reducer_status.partition_id, workerStatus.worker_id,
                    #     workerStatus.worker_address, time.asctime(time.localtime(time.time())))
                    self.finishReducer(event.status, job.reduce_task_list)
                if event.type == 'WORKER_DOWN':
                    workerStatus = event.status
                    print "Warning: worker %s %s down at %s" % (
                        workerStatus.worker_id,
                        workerStatus.worker_address, time.asctime(time.localtime(time.time())))
                    self.processWorkerDown(event.status, job)
                if event.type == 'REGISTER_WORKER':
                    worker = event.status
                    self.worker_list[worker['id']] = worker
                    # print "Register Worker: worker_id: %s, ip: %s at %s" % (
                    #     worker['id'], worker['address'], time.asctime(time.localtime(time.time())))
                    if job is not None:
                        self.assignTask('mapper', job.map_task_list)
                        self.assignTask('reducer', job.reduce_task_list)
                if event.type == 'COLLECT_SUCCESS':
                    print "Job %s completed! at %s" % (
                        job.jobId, time.asctime(time.localtime(time.time())))
                    # change job status
                    self.processing_jobs[job.jobId].state = 'COMPLETE'
                    self.processing_jobs[job.jobId].progress = '100%'
                    job.state = 'COMPLETE'
                if event.type == 'START_TASK_FAIL':
                    worker_id = event.status['worker_id']
                    type = event.status['task_type']
                    if self.worker_list.has_key(worker_id):
                        self.worker_list[worker_id][type] = 'Free'
            # print "eee"
            gevent.sleep(0)
            # print "eee3333"

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
                        self.reportEvent(Event.WORKER_DOWN, worker_id)
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
        self.event_queue.put_nowait(event)

    def updateWorkerStatus(self, worker_id, task_status_list):
        # update worker status
        if self.worker_status_list.has_key(worker_id) == False:
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
        # check status
        for key, status in task_status_list.items():
                if status == Status.FINISH :
                    if self.worker_list[worker_id] is not None :
                        self.worker_list[worker_id]['num_slots'] += 1
                    self.reportEvent(Event.FINISH_TASK, key)
                    debug_print("Report Task Finish: worker_id: %s task_id: %s at %s" % (
                    worker_id, key, time.asctime(time.localtime(time.time()))), self.debug)
        return 0

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

    def get_job(self, job):
        self.job_list[self.job_id] = unpickle_object(job)
        self.job_list[self.job_id].run(SparkDriver())
        job += 1

if __name__ == '__main__':
    status = Worker_Status.UP
    port = sys.argv[1]
    if len(sys.argv) == 3:
        if sys.argv[2] == 'debug':
            debug = True
    elif len(sys.argv) == 2:
        debug = False
    master = Master(port, debug)
    master.run()
    # rpc_server = zerorpc.Server(master)
    # addr = "tcp://0.0.0.0:" + port
    # rpc_server.bind(addr)
    # print "rpc run 1"
    # rpc_server.run()
    # print "rpc run"


