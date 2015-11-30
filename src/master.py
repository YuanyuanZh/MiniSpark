import zerorpc
import sys
import gevent
from gevent.queue import Queue

import random
from gevent.lock import *
import time
import os


from util.util_enum import *
from util.util_debug import *

class Master():
    def __init__(self, port, data_dir):
        self.port = port
        self.data_dir = data_dir
        self.worker_list = {}
        self.worker_id = -1
        self.job_id = -1
        self.jobs = Queue()
        self.processing_jobs = {}
        self.task_id = -1
        self.task_list = []
        self.worker_status_list = {}
        self.event_queue = Queue()
        self.collect_queue = Queue()


    def getNewJobID(self):
        self.job_id += 1
        return self.job_id


    def getJobStatus(self, job_id):
        job = self.processing_jobs[job_id]
        return job.state, job.progress

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

    def finishMapper(self, workerStatus, mapper_list):
        # update task status
        task = mapper_list[workerStatus.mapper_status.split_id]
        if task.state != 'NOT_ASSIGNED':
            task.state = workerStatus.mapper_status.state
            task.progress = workerStatus.mapper_status.progress
            # update worker assign state
            # print "Finish Mapper: key: %s, task_id: %s, worker_id: %s, ip: %s at %s" % (
            # task.split_id, task.task_id, task.worker['id'], task.worker['address'],
            # time.asctime(time.localtime(time.time())))
            print "Mapper %s finished. Worker_id: %s %s, at %s" % (
            task.split_id, task.worker['id'], workerStatus.worker_address, time.asctime(time.localtime(time.time())))

            self.worker_list[workerStatus.worker_id]['mapper'] = 'Free'
            # assgin new mapper task to this slot
            self.assignTask('mapper', mapper_list)


    def finishReducer(self, workerStatus, reducer_list):
        # update task status
        task = reducer_list[workerStatus.reducer_status.partition_id]
        if task.state != 'NOT_ASSIGNED':
            task.state = workerStatus.reducer_status.state
            task.progress = workerStatus.reducer_status.progress
            # update worker assign state
            self.worker_list[workerStatus.worker_id]['reducer'] = 'Free'
            # assgin new reducer task to this slot
            self.assignTask('reducer', reducer_list)
            # check if all reducers finished
            if self.isAllTasksFinished(reducer_list):
                self.collect_queue.put(reducer_list)
                # self.reportEvent('NEED_COLLECT', reducer_list)
                # ret = self.collectResults(reducer_list)
                # if ret == 0:
                #     #change job status
                #     self.processing_jobs[task.job_id].state = 'COMPLETE'
                #     self.processing_jobs[task.job_id].progress = '100%'
            # print "Finish Reducer: key: %s, task_id: %s, worker_id: %s, ip: %s at %s" % (
            #     task.partition_id, task.task_id, task.worker['id'], task.worker['address'],
            #     time.asctime(time.localtime(time.time())))
            print "Reducer %s finished. worker_id: %s %s, at %s" % (
                task.partition_id, task.worker['id'], workerStatus.worker_address,
                time.asctime(time.localtime(time.time())))
        return

    def collectJobResult(self):
        # print "enter collect : at %s" %time.asctime( time.localtime(time.time()) )
        while True:
            while not self.collect_queue.empty():
                print "Result collect started at", time.asctime(time.localtime(time.time()))
                reducer_list = self.collect_queue.get_nowait()
                ret = self.collectResults(reducer_list)
                if ret == 0:
                    self.reportEvent('COLLECT_SUCCESS', None)
                else:
                    print "Collect result failed at: ", time.asctime(time.localtime(time.time()))
            gevent.sleep(0)


    def collectResults(self, reducer_list):
        data_list = []
        for key, task in reducer_list.items():
            client = zerorpc.Client()
            client.connect('tcp://' + task.worker['address'])
            data = client.getReducerResult(task.partition_id, task.outfile)
            if data is not None:
                client.close()
                data_list.append(data)
            else:
                print "Get reduce result failed on worker: id: %s, address:%s %s" % (
                    task.worker['id'], task.worker['address'], time.asctime(time.localtime(time.time())))
                return -1
        self.mergeData(data_list, task.className, self.data_dir+'/' )
        return 0

    def processWorkerDown(self, workerStatus, job):
        # print "Processing worker down: key: worker_id: %s, ip: %s at %s" % (
        #     workerStatus.worker_id, workerStatus.worker_address, time.asctime(time.localtime(time.time())))
        # remove worker from worker list
        del self.worker_list[workerStatus.worker_id]
        del self.worker_status_list[workerStatus.worker_id]
        # find all tasks on this worker and clean
        if job is not None:
            for split_id, task in job.map_task_list.items():
                if (task.worker is not None) and (task.worker['id'] == workerStatus.worker_id):
                    task.task_id = None
                    task.worker = None
                    task.state = 'NOT_ASSIGNED'
                    task.progress = '0'
            for partition_id, task in job.reduce_task_list.items():
                if (task.worker is not None) and (task.worker['id'] == workerStatus.worker_id):
                    task.task_id = None
                    task.worker = None
                    task.state = 'NOT_ASSIGNED'
                    task.progress = '0'
        # del self.worker_list[workerStatus.worker_id]
        # del self.worker_status_list[workerStatus.worker_id]
        if job is not None:
            self.assignTask("reducer", job.reduce_task_list)
            self.assignTask('mapper', job.map_task_list)
        # else :
            # print " Job is none"

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
        event = Event(type, status)
        self.event_queue.put_nowait(event)

    def updateWorkerStatus(self, worker_id, task_status_list):
        # update worker status
        if self.worker_status_list[worker_id] is None:
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
        thread1 = gevent.spawn(self.jobScheduler)
        print "Job scheduler started at %s" %(time.asctime( time.localtime(time.time()) ))
        thread2 = gevent.spawn(self.heartBeat)
        print "Heartbeat started at %s" %(time.asctime( time.localtime(time.time()) ))
        thread3 = gevent.spawn(self.collectJobResult)
        print "Job tracker started at %s" %(time.asctime( time.localtime(time.time()) ))
        thread4 = gevent.spawn(self.rpcServer)
        print "RPC server started at %s" %(time.asctime( time.localtime(time.time()) ))
        gevent.joinall([thread1,thread2,thread3,thread4])
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

if __name__ == '__main__':
    port = sys.argv[1]
    data_dir = sys.argv[2]
    master = Master(port, data_dir)
    master.run()
    # rpc_server = zerorpc.Server(master)
    # addr = "tcp://0.0.0.0:" + port
    # rpc_server.bind(addr)
    # print "rpc run 1"
    # rpc_server.run()
    # print "rpc run"


