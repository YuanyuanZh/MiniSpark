import zerorpc
import sys
import gevent
from gevent.queue import Queue

import random
from gevent.lock import *
import time
import os


from util.util_enum import *

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

    def splitInput(self, infile,split_size,classname):
        if classname == 'hammingEnc' or classname == 'hammingDec' or classname == 'hammingFix':
            split_hashmap = input_split.hammingSplit(infile,split_size,classname).generate_split_info()
        else:
            split_hashmap = input_split.Split(infile,split_size,classname).generate_split_info()
        return split_hashmap

    def submitJob(self, conf):
        # create job
        inputFile = self.data_dir + '/' + conf['infile']
        outputFile = self.data_dir + '/' + conf['outfile']
        e = os.path.exists(inputFile)
        if e == False:
            raise IOError,"No input file"
        splits = self.splitInput(inputFile, conf['split_size'], conf['className'])
        conf['infile'] = inputFile
        conf['outfile'] = outputFile
        conf['splits'] = splits
        job = Job(conf)
        self.jobs.put_nowait(job)
        print "Initialize Job: job_id: %s at %s" % (job.jobId, time.asctime(time.localtime(time.time())))
        return 0

    def getJobStatus(self, job_id):
        job = self.processing_jobs[job_id]
        return job.state, job.progress

    def registerWorker(self, worker_address):
        self.worker_id += 1
        worker = {
            "id": self.worker_id,
            "address": worker_address,
            "mapper": 'Free',
            "reducer": 'Free'
        }
        # self.worker_list[self.worker_id] = worker
        self.reportEvent('REGISTER_WORKER', worker)
        self.worker_status_list[self.worker_id] = WorkerStatus(self.worker_id, worker_address, "RUNNING", None, None)
        print "Worker %s %s registered at %s" % (
            worker['id'], worker['address'], time.asctime(time.localtime(time.time())))
        return self.worker_id

    def getMapSlot(self):
        for worker_id in self.worker_list.keys():
            if self.worker_list[worker_id]['mapper'] == 'Free':
                self.worker_list[worker_id]['mapper'] = 'Occupied'
                return self.worker_list[worker_id]
        return None

    def getReduceSlot(self):
        for key in self.worker_list.keys():
            if self.worker_list[key]['reducer'] == 'Free':
                self.worker_list[key]['reducer'] = 'Occupied'
                return self.worker_list[key]
        return None

    def getMapResultLocation(self, job_id):
        locations = {}
        # find corresponding map result
        job = self.processing_jobs[job_id]
        map_list = job.map_task_list
        for key, task in map_list.items():
            # task = map_list[i]
            if task.state == 'FINISH':
                locations[task.split_id] = task.worker
        return locations

    def assignTask(self, type, task_list):
        for i in task_list.keys():
            if task_list[i].state == 'NOT_ASSIGNED':
                if type == 'mapper':
                    # print " Get M Slot"
                    worker = self.getMapSlot()
                else:
                    worker = self.getReduceSlot()
                    # print " Get R Slot"

                if worker is not None:
                    # print "worker %s " %worker['id']
                    # print "Task TYPE %s " %type

                    task = task_list[i]
                    self.task_id += 1
                    task.task_id = self.task_id
                    task.worker = worker
                    # start task
                    client = zerorpc.Client()
                    client.connect('tcp://' + worker["address"])
                    if type == 'mapper':
                        task_dict = task.__dict__
                        print "Assign mapper task %s on worker %s at %s " %(task.split_id , worker['id'],
                            time.asctime(time.localtime(time.time())))
                        try:
                            ret = client.startMap(task_dict)
                            if ret is not None:
                                client.close()
                        except zerorpc.LostRemote:
                            ret = -1
                            pass
                        # print "Start Mapper OK %s on worker %s" %(task.split_id, worker['id'])

                    else:
                        task_dict = task.__dict__
                        # print "Start Reducer task"
                        print "Assign reducer task %s on worker %s at %s " %(task.partition_id , worker['id'],
                            time.asctime(time.localtime(time.time())))
                        try:
                            ret = client.startReduce(task_dict)
                            if ret is not None:
                                client.close()
                        except zerorpc.LostRemote:
                            ret = -1
                            pass
                    if ret == 0:
                        task.state = 'STARTING'
                    else:
                        print "Start %s task on %s failed" % (type, worker["address"])
                        # worker[type] = "Free"
                        status = {
                                'worker_id': task.worker['id'],
                                'task_type': type
                                  }
                        self.reportEvent('START_TASK_FAIL', status)

    def isAllTasksFinished(self, task_list):
        count = 0
        for key in task_list.keys():
            if task_list[key].state == 'FINISH':
                count += 1
        if count == len(task_list):
            return True
        else:
            return False

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

    def mergeData(self, data_list, className, data_dir):
        collector = collect_data.Collect_data(className, className, data_dir)
        collector.merge_data(data_list, className)
        return

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
                workerStatus = self.worker_status_list[worker_id]
                if workerStatus.num_heartbeat == workerStatus.num_callback:
                    workerStatus.timeout_times += 1
                    if workerStatus.timeout_times == 3:
                        workerStatus.worker_status = 'DOWN'
                        self.reportEvent('WORKER_DOWN', workerStatus)
                        # print "Find worker down: worker_id: %s, ip: %s at %s" % (
                        #     workerStatus.worker_id, workerStatus.worker_address,
                        #     time.asctime(time.localtime(time.time())))
                else:
                    workerStatus.num_heartbeat = workerStatus.num_callback
                    workerStatus.timeout_times = 0
            gevent.sleep(2)

    def reportEvent(self, type, status):
        event = Event(type, status)
        self.event_queue.put_nowait(event)

    def updateWorkerStatus(self, worker_id, task_status_list):
        # workerStatus = self.convertDictToWorkerStatus(workerStatus_dict)
        # print " call back"
        # check status
        origin_status = self.worker_status_list[worker_id]
        if origin_status is None:
            # create this worker task if not exist
            status = {
                'worker_status': Worker_Status.UP,
                'task_status_list':
            }
        if origin_status['worker_status'] != 'Down':
            origin_status['task_status_list'] = task_status_list
            for key, status in task_status_list.items():
                if status == Status.FINISH :


            if workerStatus.mapper_status is not None:
                # check task state
                if workerStatus.mapper_status.changeToFinish == True:
                    # print "Mapper finish key: %d worker : %d" %(workerStatus.mapper_status.split_id, workerStatus.worker_id)
                    self.reportEvent('MAPPER_FINISHED', workerStatus)

            if workerStatus.reducer_status is not None:
                if workerStatus.reducer_status.changeToFinish == True:
                    self.reportEvent('REDUCER_FINISHED', workerStatus)
        self.worker_status_list[workerStatus.worker_id].mapper_status = workerStatus.mapper_status
        self.worker_status_list[workerStatus.worker_id].reducer_status = workerStatus.reducer_status
        self.worker_status_list[workerStatus.worker_id].num_callback = random.random()
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


