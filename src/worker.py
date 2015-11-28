import zerorpc
import gevent
import socket

import sys
import time
import json
from gevent.queue import Queue


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
        self.all_map_task_list = {}
        self.all_reduce_task_list = {}
        self.current_mapper = None
        self.current_reducer = None
        self.MapperTaskQueue = Queue()
        self.ReducerTaskQueue = Queue()

    def mapper(self, map_task):
        task = map_task
        if self.current_mapper is not None:
            if self.current_mapper.state != 'FINISH':
                print "Create mapper failed: last mapper not finished. key: %s, task_id: %s  at %s" % (
                    task.split_id, task.task_id, time.asctime(time.localtime(time.time())))
                return -1
            else:
                self.current_mapper.changeToFinish = False
        self.current_mapper = task
        # No this job, create it
        if self.all_map_task_list.has_key(task.job_id) == False:
            map_list = {}
            map_list[task.split_id] = task
            self.all_map_task_list[task.job_id] = map_list
        else:
            self.all_map_task_list[task.job_id][task.split_id] = task

        task.state = 'STARTING'
        task.progress = 'Starting.'
        if task.className == 'WordCount':
            engine = WordCountEngine(task.infile, task.num_reducers, task.outfile)
            partitions = engine.map_execute(task.splits, task.split_id)
        if task.className == 'Sort':
            engine = SortEngine(task.infile, task.num_reducers, task.outfile)
            partitions = engine.map_execute(task.splits, task.split_id)
        if task.className == 'hammingEnc' or task.className == 'hammingDec' or task.className == 'hammingFix' \
                or task.className == 'hammingChk' or task.className == 'hammingErr':
            engine = HammingEngine(task.infile, task.num_reducers, task.outfile)
            partitions = engine.map_execute(task.splits, task.className, task.split_id)
        task.partitions = partitions
        task.state = 'FINISH'
        task.progress = 'Finish mapping.'
        task.changeToFinish = True
        print "Mapper finished: Key: %s at %s" % (
            task.split_id, time.asctime(time.localtime(time.time())))

    def collectAllInputsForReducer(self, task):
        # get all input from other workers
        count = 0
        while True:
            try:
                client1 = zerorpc.Client()
                client1.connect('tcp://' + self.master_address)
                # get input locations from master
                locations = client1.getMapResultLocation(task.job_id)
                if locations is not None:
                    client1.close()
                    for split_id, worker in locations.items():
                        # if didn't has that input, get input from mapper; else ignore;
                        key = str(split_id) + str(task.partition_id)
                        if task.partitions.has_key(key) == False:
                            client = zerorpc.Client(timeout=300)
                            client.connect('tcp://' + worker['address'])
                            partition = client.getPartition(task.job_id, split_id, task.partition_id)
                            if partition is not None:
                                client.close()
                                task.partitions[key] = partition
                                # print "Get partition %s from worker %s successfully at %s" % (
                                #     key, worker['address'], time.asctime(time.localtime(time.time())))
                            else:
                                print "Get partition %s from worker %s failed at %s" % (
                                    key, worker['address'], time.asctime(time.localtime(time.time())))
            except zerorpc.LostRemote:
                print "RPC error: lost remote"
                pass

            # Get all inputs
            if len(task.partitions) == task.num_mappers:
                # print "Get all partitions for reducer: key: %s, task_id: %s, length: %d at %s" % (
                #     task.partition_id, task.task_id, len(task.partitions),
                #     time.asctime(time.localtime(time.time())))
                return 0
            count += 1
            # print "reducer wait for input times: %d" %count
            gevent.sleep(5)

    def reducer(self, reduce_task):
        task = reduce_task
        if self.current_reducer is not None:
            if self.current_reducer.state != 'FINISH':
                print "Create reducer failed: last reducer not finished. key: %s, task_id: %s  at %s" % (
                    task.partition_id, task.task_id, time.asctime(time.localtime(time.time())))
                return -1
        self.current_reducer = task
        # No this job, create it
        if self.all_reduce_task_list.has_key(task.job_id) == False:
            reduce_list = {}
            reduce_list[task.partition_id] = task
            self.all_reduce_task_list[task.job_id] = reduce_list
        else:
            self.all_reduce_task_list[task.job_id][task.partition_id] = task

        task.state = 'STARTING'
        task.progress = 'Starting.'

        self.collectAllInputsForReducer(task)

        if task.className == 'WordCount':
            # reducer = mr_classes.WordCountReduce()
            engine = WordCountEngine(task.infile, task.num_reducers, task.outfile)
        if task.className == 'Sort':
            engine = SortEngine(task.infile, task.num_reducers, task.outfile)
        if task.className == 'hammingEnc' or task.className == 'hammingDec' or task.className == 'hammingFix' \
                or task.className == 'hammingChk' or task.className == 'hammingErr':
            engine = HammingEngine(task.infile, task.num_reducers, task.outfile)

        # print "input partitions: ",task.partitions
        engine.reduce_execute(task.partitions, task.partition_id)

        task.state = 'FINISH'
        task.progress = 'Finish reducing.'
        task.changeToFinish = True
        print "Reducer finished: Key: %s at %s" % (
            task.partition_id, time.asctime(time.localtime(time.time())))

    def getMyAddress(self):
        try:
            csock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            csock.connect(('8.8.8.8', 80))
            (addr, port) = csock.getsockname()
            csock.close()
            return addr + ":" + port
        except socket.error:
            return "127.0.0.1"

    def getPartition(self, job_id, split_id, partition_id):
        if self.all_map_task_list.has_key(job_id):
            if self.all_map_task_list[job_id].has_key(split_id):
                key = str(split_id) + str(partition_id)
                # print "Prepare get partition %s" %(key)
                # print "Now partition keys: ", self.all_map_task_list[job_id][split_id].partitions.keys()
                return self.all_map_task_list[job_id][split_id].partitions[key]
        return None

    def getReducerResult(self, partition_id, outfile_base):
        filename = outfile_base + "_" + str(partition_id) + ".json"
        with open(filename, 'r') as f:
            data = json.load(f)
            # print "Get reduce file success, partition id:", partition_id
        return data

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

    def register(self):
        client = zerorpc.Client()
        client.connect('tcp://' + self.master_address)
        self.id = client.registerWorker(self.worker_address)
        if self.id is not None:
            client.close()
            addr = self.worker_address
            print "worker %d  %s registered at %s " % (self.id, addr, time.asctime(time.localtime(time.time())))


    def convertDictToMapTask(self, dict):
        task = MapTask(dict['job_id'], dict['split_id'], dict['task_id'], dict['className'], dict['worker'],
                       dict['splits'], dict['infile'], dict['partitions'], dict['num_reducers'], dict['outfile'])
        return task

    def convertDictToReduceTask(self, dict):
        task = ReduceTask(dict['job_id'], dict['partition_id'], dict['task_id'], dict['className'], dict['worker'],
                          dict['partitions'], dict['outfile'], dict['num_mappers'], dict['infile'],
                          dict['num_reducers'])
        return task

    def startMap(self, task_dict):
        # print "Begin create map thread: at %s" % (time.asctime(time.localtime(time.time())))
        task = self.convertDictToMapTask(task_dict)
        # thread = gevent.spawn(self.mapper, task)
        # print "Create map thread: %s at %s" % (thread, time.asctime(time.localtime(time.time())))
        self.MapperTaskQueue.put(task)
        return 0

    def startReduce(self, task_dict):
        task = self.convertDictToReduceTask(task_dict)
        self.ReducerTaskQueue.put(task)
        # gevent.spawn(self.reducer(task))
        return 0

    def MapperManage(self):
        while True:
            while not self.MapperTaskQueue.empty():
                mapperTask = self.MapperTaskQueue.get()
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
