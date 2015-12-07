import gevent
from gevent.queue import Queue

import random
import time
from src.client.basicclient import StreamingClient
from src.driver import SparkDriver
from src.driver import StreamingDriver

from util.util_enum import *
from util.util_debug import *
from util.util_pickle import *
from util.util_zerorpc import *
from gevent.event import AsyncResult


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
        self.task_event_list = {}
        self.worker_event_list = {}
        self.streaming_data = {}

    def registerWorker(self, worker_address):
        """
        Receive register  worker request. Add event to event queue.
        Event handler would process registration event later.
        :param worker_address:
        :return: worker id
        """
        self.worker_id += 1
        worker_id = self.worker_id
        self.worker_event_list[worker_id] = AsyncResult()
        event_object = {
            'worker_id': worker_id,
            'worker_address': worker_address
        }
        self.reportEvent(Event.REGISTER, event_object)
        debug_print("Report Worker %s %s registration at %s" % (
            worker_id, worker_address, time.asctime(time.localtime(time.time()))), self.debug)
        # wait for
        self.worker_event_list[worker_id].get()
        return worker_id

    def register_worker_execute(self, worker_id, worker_address):
        """
        Process registration event.
        :param worker_id:
        :param worker_address:
        :return:
        """
        worker = {
            "worker_id": worker_id,
            "address": worker_address,
            'num_slots': 5
        }
        self.worker_list[worker_id] = worker
        debug_print("Process worker %s %s registered at %s" % (
            worker['worker_id'], worker['address'], time.asctime(time.localtime(time.time()))), self.debug)
        self.worker_event_list[worker_id].set()

    def get_available_worker(self):
        """
        Return worker which has free slots on it. It is called by driver.
        :return:
        """
        candidate_worker = None
        max_slot = 0
        for worker_id, worker in self.worker_list.items():
            if worker['num_slots'] > max_slot:
                max_slot = worker['num_slots']
                candidate_worker = worker
        return candidate_worker

    def get_worker_list(self):
        """
        Return all worker list
        :return:
        """
        return self.worker_list

    def update_task_node_table(self, worker_id, task_node_table):
        """
        Update task and worker mapping table on specified worker.
        :param worker_id:
        :param task_node_table:
        :return:
        """
        worker_address = self.worker_list[worker_id]['address']
        client = get_client(worker_address)
        ret = execute_command(client, client.update_task_node_table, task_node_table)
        return ret

    def assign_task(self, worker_id, task, task_node_table):
        """
        Receive assign task request from driver. Add event to queue. Wait event handler to process it.
        :param worker_id:
        :param task:
        :param task_node_table:
        :return: 0: assign task succesfully
                 1: assign task failed
        """
        key = '{0}_{1}'.format(task.job_id, task.task_id)
        self.task_event_list[key] = AsyncResult()
        event_object = {
            'worker_id': worker_id,
            'task': task,
            'task_node_table': task_node_table
        }
        self.reportEvent(Event.ASSIGN_TASK, event_object)
        debug_print("[Master] Report assign task %s at %s" % (
            task.task_id, time.asctime(time.localtime(time.time()))), self.debug)
        ret = self.task_event_list[key].get()
        return ret

    def assign_task_execute(self, worker_id, task, task_node_table):
        """
        Process assigning task. Call assign task rpc on worker.
        :param worker_id:
        :param task:
        :param task_node_table:
        :return: 0: assign task on worker succesfully
                 1: assign task on worker failed
        """
        task_str = pickle_object(task)
        worker_address = self.worker_list[worker_id]['address']
        debug_print(
            "[Master] Sending Task {0} to Worker {1}, address {2}".format(task.task_id, worker_id, worker_address),
            self.debug)
        client = get_client(worker_address)
        ret = execute_command(client, client.start_task, task_str, task_node_table)
        debug_print("[Master] Sent Task {0} to Worker {1} with return val {2}".format(task.task_id, worker_id, ret),
                    self.debug)

        if ret == 0:
            self.worker_list[worker_id]['num_slots'] -= 1
            debug_print("[Master] Assign task successfully: worker_id: %s job: %s task: %s at %s" % (
                worker_id, task.job_id, task.task_id, time.asctime(time.localtime(time.time()))), self.debug)
        else:
            ret = 1
        key = '{0}_{1}'.format(task.job_id, task.task_id)
        self.task_event_list[key].set(ret)

    def get_rdd_result(self, task, worker_info, partition_id):
        """
        Get rdd execution result from worker. This is called by driver
        :param task:
        :param worker_info:
        :param partition_id:
        :return: RDD execution data
        """
        worker_address = worker_info['address']
        job_id = task.job_id
        task_id = task.task_id
        client = get_client(worker_address)
        data = execute_command(client, client.get_rdd_result, job_id, task_id, partition_id)
        debug_print("[Master] Get RDD result task: job: %s task: %s at %s" % (
            job_id, task_id, time.asctime(time.localtime(time.time()))), self.debug)
        return data

    def heartBeat(self):
        """
        This is a thread to detect worker status.

        """
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
                        # if self.worker_list.has_key(worker_id) :
                        #     if self.worker_list[worker_id] is not None :
                        #         del self.worker_list[worker_id]
                        #         # self.worker_list[worker_id]['num_slots'] == 0
                        # for job_id in self.driver_list.keys():
                        #     self.driver_list[job_id][0].fault_handler(worker_id)
                        # # self.reportEvent(Event.WORKER_DOWN, worker_id)
                        debug_print("Report Worker Down: worker_id: %s at %s" % (
                            worker_id, time.asctime(time.localtime(time.time()))), self.debug)
                        # print "Find worker down: worker_id: %s, ip: %s at %s" % (
                        #     workerStatus.worker_id, workerStatus.worker_address,
                        #     time.asctime(time.localtime(time.time())))
                else:
                    status['num_heartbeat'] = status['num_callback']
                    status['timeout_times'] = 0
            gevent.sleep(2)

    def reportEvent(self, type, event_object):
        """
        Create event and put to event queue
        :param type:
        :param event_object:
        """
        event = {'type': type,
                 'event_object': event_object
                 }

        self.event_queue.put(event)

    def process_worker_down(self, worker_id):
        """
        Process worker down event. Remove this worker from worker list. Notify driver to process worker down.
        :param worker_id:
        :return:
        """
        if self.worker_list.has_key(worker_id):
            if self.worker_list[worker_id] is not None:
                del self.worker_list[worker_id]
                # self.worker_list[worker_id]['num_slots'] == 0
                for job_id in self.driver_list.keys():
                    self.driver_list[job_id][0].fault_handler(worker_id)
                    # self.reportEvent(Event.WORKER_DOWN, worker_id)
                    debug_print("Process Worker Down: worker_id: %s at %s" % (
                        worker_id, time.asctime(time.localtime(time.time()))), self.debug)

    def finish_task_execute(self, job_id, task_id, worker_id):
        """
        Process finishing task event. Decrease slot on corresponding worker. Notify drive to process.
        :param job_id:
        :param task_id:
        :param worker_id:
        """
        if self.worker_list[worker_id] is not None:
            self.worker_list[worker_id]['num_slots'] += 1
            driver = self.find_driver(job_id)
            if driver is not None:
                driver.finish_task(task_id)
                # self.reportEvent(Event.FINISH_TASK, key)
                debug_print("Process Task Finish: worker_id: %s job_id %s task_id: %s at %s" % (
                    worker_id, job_id, task_id, time.asctime(time.localtime(time.time()))), self.debug)

    def event_handler(self):
        """
        A thread to process event.
        """
        while True:
            while not self.event_queue.empty():
                event = self.event_queue.get()
                if event['type'] == Event.REGISTER:
                    self.register_worker_execute(event['event_object']['worker_id'],
                                                 event['event_object']['worker_address'])
                if event['type'] == Event.ASSIGN_TASK:
                    event_object = event['event_object']
                    worker_id = event_object['worker_id']
                    task = event_object['task']
                    task_node_table = event_object['task_node_table']
                    self.assign_task_execute(worker_id, task, task_node_table)
                if event['type'] == Event.FINISH_TASK:
                    event_object = event['event_object']
                    job_id = event_object['job_id']
                    task_id = event_object['task_id']
                    worker_id = event_object['worker_id']
                    self.finish_task_execute(job_id, task_id, worker_id)
                if event['type'] == Event.WORKER_DOWN:
                    self.process_worker_down(event['event_object'])
            gevent.sleep(0)

    def updateWorkerStatus(self, worker_id, task_status_list):
        """
        Receive status update from worker. Process status and report event if a task is finished.
        :param worker_id:
        :param task_status_list:
        :return: 0: update worker status on master succesfully.
        """
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
                if status == Status.FINISH:
                    event_object = {
                        'job_id': job_id,
                        'task_id': task_id,
                        'worker_id': worker_id
                    }
                    self.reportEvent(Event.FINISH_TASK, event_object)
                    # if self.worker_list[worker_id] is not None:
                    #     self.worker_list[worker_id]['num_slots'] += 1
                    # driver = self.find_driver(job_id)
                    # if driver is not None:
                    #     driver.finish_task(task_id)
                    #     # self.reportEvent(Event.FINISH_TASK, key)
                    debug_print("Report Task Finish: worker_id: %s job_id %s task_id: %s at %s" % (
                        worker_id, job_id, task_id, time.asctime(time.localtime(time.time()))), self.debug)
        return 0

    def find_driver(self, job_id):
        if self.driver_list.has_key(job_id):
            return self.driver_list[job_id][0]
        else:
            return None

    def run(self):
        thread1 = gevent.spawn(self.heartBeat)
        debug_print("[Master] Heartbeat started at %s" % (time.asctime(time.localtime(time.time()))), self.debug)
        thread2 = gevent.spawn(self.rpcServer)
        debug_print("[Master] RPC server started at %s" % (time.asctime(time.localtime(time.time()))), self.debug)
        thread3 = gevent.spawn(self.event_handler)
        debug_print("[Master] Event handler started at %s" % (time.asctime(time.localtime(time.time()))), self.debug)
        gevent.joinall([thread1, thread2, thread3])

    def rpcServer(self):
        """
        Create a rpc server to listen on request.

        """
        rpc_server = zerorpc.Server(self)
        addr = "tcp://0.0.0.0:" + self.port
        rpc_server.bind(addr)
        rpc_server.run()


    def run_loop_job(self, job_id, driver):
        while True:
            gevent.spawn(self.job_list[job_id].run, driver)
            gevent.sleep(self.job_list[job_id].interval)


    def get_job(self, job, client_address):
        """
        Callback from a client. Receive a job object and create a driver for each job. Start a driver to execute.
        :param job: job object
        :param client_address:
        :return: job id
        """
        job_id = self.job_id
        self.job_list[job_id] = unpickle_object(job)
        if isinstance(self.job_list[job_id], StreamingClient):
            driver = StreamingDriver(job_id)
            gevent.spawn(self.job_list[job_id].run, driver)
        else:
            driver = SparkDriver(job_id)
            gevent.spawn(self.run_loop_job, job_id, driver)

        self.driver_list[job_id] = (driver, client_address)
        self.job_id += 1
        return self.job_id

    def return_client(self, job_id, result):
        """
        Return job result to client. This is called by driver when job is finished.
        :param job_id:
        :param result: job result
        :return:
        """
        if self.driver_list.has_key(job_id):
            client_address = self.driver_list[job_id][1]
            client = get_client(client_address)
            debug_print("[Master] Finish job: %s for client %s at %s" % (
                job_id, client_address, time.asctime(time.localtime(time.time()))), self.debug)
            execute_command(client, client.recieve_msg, 'Finish job with result: {0}'.format(result))

    def send_partition(self, streaming_data):
        job_id, worker_id, partition_id = streaming_data.split(',')
        if job_id not in self.streaming_data:
            self.streaming_data[job_id] = {}
        if worker_id not in self.streaming_data:
            self.streaming_data[job_id][worker_id] = []
        if partition_id not in self.streaming_data[worker_id]:
            self.streaming_data[job_id][worker_id].append(partition_id)

        print(self.streaming_data)

if __name__ == '__main__':
    status = Worker_Status.UP
    port = sys.argv[1]
    if len(sys.argv) == 3:
        if sys.argv[2] == 'debug':
            debug = True
    elif len(sys.argv) == 2:
        debug = False
    master = Master(port, debug)
    SparkDriver._master = master
    master.run()
