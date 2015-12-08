import gevent
import zerorpc
from src.rdd.rdd import WideRDD, TextFile, GroupByKey, Map, Join, \
    MultiParentNarrowRDD, InputRDD, NarrowRDD, Streaming
from src.task import Task
import time
from src.util.util_debug import *


class SparkDriver:
    _master = None

    def __init__(self, job_id):
        self.actions = {"reduce": self.do_reduce,
                        "collect": self.do_collect,
                        "count": self.do_count
                        }
        # task_list: {task: status}
        self.job_id = job_id
        self.task_list = {}
        self.args = None
        # task_node_table: {worker_id: [tasks]}
        self.task_node_table = {}
        self.func = None
        self.action = None
        self.result = []
        self.isFinished = False
        # self.result_ready = gevent.event.Event()
        # self.result_ready.clear()

    def assign_task_list(self, tasks):
        for task in tasks:
            # gevent.spawn(self.assign_task, task)
            ret = self.assign_task(task)
            while ret is not 0:
                gevent.sleep(0.5)
                ret = self.assign_task(task)

            self.updata_all_node_table()

    def do_drive(self, last_rdd, action_name, *args):
        self.action = self.actions[action_name]
        lineage = last_rdd.get_lineage()
        print_infor(bcolors.OKGREEN, "[SparkDriver]*****************", self._master.debug)
        print_infor(bcolors.OKGREEN, "[SparkDriver]*** Partition Form:", self._master.debug)
        for rdd in lineage:
            print_infor(bcolors.OKGREEN, "[SparkDriver]***{0}    {1}".format(rdd[1], rdd[
                0].partitions()), self._master.debug)
        print_infor(bcolors.OKGREEN, "[SparkDriver]*****************", self._master.debug)
        self.args = args
        # generate graph-table and stages
        # partition_graph = self.gen_graph_table(last_rdd)
        self.init_tasks(lineage)
        # Do some fuction to generate the rdd that apply the operation and the result
        tasks = self.task_list.keys()
        tasks.sort(lambda x, y: cmp(int(x.task_id.split("_")[0]),
                                    int(y.task_id.split("_")[0])))
        # print "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX{0}".format(tasks[0].task_id)
        gevent.spawn(self.assign_task_list, tasks)

        # self.result_ready.wait()
        # return self.action(args)

    def result_collected_notify(self, event):
        self.result_ready.set()

    def fault_handler(self, worker_id):
        print_infor(bcolors.OKGREEN, "[Worker] Worker {0} is down!".format(worker_id))
        task_node_table_keys = filter(
            lambda a: self.task_node_table[a]["worker_id"] == worker_id,
            self.task_node_table.keys())
        task_node_table_keys.sort(
            lambda a, b: cmp(int(a.split('_')[1]), int(b.split('_')[1])))
        task_list = []

        for job_task_id in task_node_table_keys:
            task_id = job_task_id.split('_', 1)[1]
            for task_i in self.task_list.keys():
                if task_i.task_id == task_id:
                    task_list.append(task_i)
                    break

        gevent.spawn(self.assign_task_list, task_list)

    def updata_all_node_table(self):
        for worker_id in self._master.worker_list.keys():
            self._master.update_task_node_table(worker_id,
                                                self.task_node_table)

    def assign_task(self, task):
        """Assign the stages list to Master Node,
           return the last rdd that action should be applied"""
        print_infor(bcolors.OKGREEN, "[SparkDriver] Assigning Task {0}...".format(task.task_id), self._master.debug)

        worker_info = None
        while worker_info is None:
            gevent.sleep(0.5)
            worker_info = self._master.get_available_worker()

        unique_task_id = '{job_id}_{task_id}'.format(job_id=self.job_id,
                                                     task_id=task.task_id)
        self.task_node_table[unique_task_id] = worker_info

        ret = self._master.assign_task(worker_info['worker_id'], task,
                                       self.task_node_table)
        if ret == 0:
            self.task_list[task] = "Assigned"
        print_infor(bcolors.OKGREEN, "[SparkDriver] Assigning Task {0}... Finished".format(
            task.task_id), self._master.debug)
        # print_infor(bcolors.OKGREEN,
        #     "[SparkDriver] Task {0}.input_resource {1}".format(task.task_id,
        #                                                        task.input_source),
        #     self._master.debug)
        return ret

    def init_tasks(self, lineage):
        """
        Generate task list for works
        :param lineage: The lineage of RDDs
        """
        print_infor(bcolors.OKGREEN, "[SparkDriver] Init Tasks", self._master.debug)
        self.task_list = {}
        cur_stage_id = 0
        prev = lineage[0][0]
        stage_start = lineage[0][0]
        for rdd, rdd_id in lineage[1:]:
            if isinstance(rdd, WideRDD) or isinstance(rdd,
                                                      InputRDD) or isinstance(
                    rdd, MultiParentNarrowRDD):
                self.task_list.update(
                    self.gen_stage_tasks(stage_start, prev, cur_stage_id))
                cur_stage_id += 1
                stage_start = rdd
            prev = rdd

        # Handle the last stage
        self.last_tasks = self.gen_stage_tasks(stage_start, prev, cur_stage_id)
        self.task_list.update(self.last_tasks)

    def gen_stage_tasks(self, start_rdd, last_rdd, cur_stage_id):
        """
        Generate a single task for a stage
        :param last_rdd: The last Rdd of stage
        :param start_rdd: The first Rdd of stage
        :param cur_stage_id: The ID of current stage
        :return: The list of tasks generated by the stage
        """
        print_infor(bcolors.OKGREEN,
            "[SparkDriver] Generating Task for Stage {0} for RDD {1}".format(
                cur_stage_id, last_rdd.id), self._master.debug)

        tasks = {}
        graph_table = last_rdd.partitions()
        for cur_par_id in range(0, len(graph_table)):
            new_task_id = self.generate_task_id(cur_stage_id, cur_par_id)

            if isinstance(start_rdd, InputRDD):
                # TextFile data source
                input_source = [{'job_id': self.job_id, "partition_id": cur_par_id}]
            elif isinstance(start_rdd, WideRDD) or isinstance(start_rdd,
                                                              MultiParentNarrowRDD):
                # Shuffle data source\
                input_source = []
                if not isinstance(start_rdd.parent, list):
                    parents = [start_rdd.parent]
                else:
                    parents = start_rdd.parent
                for parent_rdd in parents:
                    # parent_graph: [['0_0', '0_1'],['1_0','1_1']]
                    parent_graph = parent_rdd.partitions()
                    for tar_list in parent_graph:
                        for elem in tar_list:
                            if int(elem.split('_')[1]) == cur_par_id:
                                elem_dict = {'job_id': self.job_id,
                                             # 'task_id': "{0}_{1}".format(cur_stage_id, cur_par_id),
                                             'partition_id': cur_par_id}
                                # Find the Parent Task ID
                                keys = self.task_list.keys()
                                keys.sort(lambda x, y: cmp(
                                    int(x.task_id.split("_")[0]),
                                    int(y.task_id.split("_")[0])))
                                for task in keys:
                                    if task.last_rdd.id == parent_rdd.id and \
                                                    elem.split('_')[0] == \
                                                    task.task_id.split('_')[1]:
                                        print_infor(bcolors.OKGREEN,
                                            "[SparkDriver]Find Shuffle Parent Task {0} For Task{1} ".format(
                                                task.task_id,
                                                new_task_id
                                            )
                                        )
                                        elem_dict['task_id'] = task.task_id
                                input_source.append(elem_dict)

            tasks.update({Task(last_rdd, input_source, new_task_id, self.job_id): 'New'})
        return tasks

    def generate_task_id(self, cur_stage_id, cur_par_id):
        return "{0}_{1}".format(cur_stage_id, cur_par_id)

    def print_task_list(self, task_list):
        for task, status in task_list.items():
            strs = ""
            strs += '{0} : {1}'.format(
                str(task),
                status
            )
            debug_print_by_name('wentao', strs)

    def finish_task(self, task_id):
        if not self.isFinished:
            print_infor(bcolors.OKGREEN, "[SparkDriver] Task {0} Finished!".format(task_id),
                        self._master.debug)
            for task in self.task_list.keys():
                if task.task_id == task_id:
                    self.task_list[task] = "Finished"
                    print_infor(bcolors.OKGREEN,
                        "[SparkDriver] Task {0} for Job {1} Finished".format(
                            task_id, self.job_id))
                    break
            self.print_task_list(self.task_list)
            if task not in self.last_tasks.keys():
                return
            for task in self.last_tasks.keys():
                if self.task_list[task] is not 'Finished':
                    return
            if not isinstance(self, StreamingDriver):
                self.isFinished = True
            print_infor(bcolors.OKGREEN, "[SparkDriver] Job {0} Finished".format(self.job_id),
                        self._master.debug)
            # collect_process = gevent.spawn(self.get_all_results)
            # collect_process.link(self.result_collected_notify)
            # self.result_ready.wait()
            self.get_all_results()
            self.action(self.args)
            # self._master.clean_job(self.job_id)

    def get_all_results(self):
        self.result = []
        # print_infor(bcolors.OKGREEN, "[SparkDriver] Collecting Results From Nodes",
        #             self._master.debug)

        for i in range(0, len(self.last_tasks)):
            # gevent.spawn(self.get_result, self.last_tasks.keys()[i], i)
            self.get_result(self.last_tasks.keys()[i], i)

    def get_result(self, task, part_id):
        table_id = "{0}_{1}".format(self.job_id, task.task_id)
        worker_info = self.task_node_table[table_id]
        result = self._master.get_rdd_result(task, worker_info, part_id)
        # print_infor(bcolors.OKGREEN,
        #     "[SparkDriver] Collecting Result for Task {0} Part{1} :{2}".format(
        #         task.task_id, part_id, result), self._master.debug)
        self.result += result

    def do_reduce(self, args):
        func = args[0]
        reduce_result = reduce(func, self.result)
        # print_infor(bcolors.OKGREEN,
        #     "[SparkDriver] The result of REDUCE in Job {0} is :{1}".format(
        #         self.job_id, reduce_result), self._master.debug)
        self._master.return_client(self.job_id, reduce_result)

    def do_collect(self, args=None):
        # print_infor(bcolors.OKGREEN,
        #     "[SparkDriver] The result of COLLECT in Job {0} is :{1}".format(
        #         self.job_id, self.result), self._master.debug)
        self._master.return_client(self.job_id, self.result)

    def do_count(self, args=None):
        count_result = len(self.result)
        # print_infor(bcolors.OKGREEN,
        #     "[SparkDriver] The result of COUNT in Job {0} is :{1}".format(
        #         self.job_id, count_result), self._master.debug)
        self._master.return_client(self.job_id, count_result)


class StreamingDriver(SparkDriver):
    def __init__(self, job_id, interval):
        self.actions = {"reduce": self.do_reduce,
                        "collect": self.do_collect,
                        "count": self.do_count
                        }
        # task_list: {task: status}
        self.job_id = job_id
        self.task_list = {}
        self.args = None
        # task_node_table: {worker_id: [tasks]}
        self.task_node_table = {}
        self.func = None
        self.action = None
        self.result = []
        self.isFinished = False
        self.interval = interval
        # partition_info {worker_id :[partitions]}
        self.partition_infor = {}

        # partition_leader {partiton : worker_id}
        self.partition_leader = {}

        self.num_partition=len(self._master.worker_list.keys())

    def set_partition(self):

        def increase_number(value, mod):
            return (value + 1) % mod

        self.partition_infor = {}
        worker_number = len(self._master.worker_list)
        self.num_partition=worker_number
        worker_id_keys = self._master.worker_list.keys()
        for index in xrange(0, worker_number):
            worker_id = worker_id_keys[index]
            second_index = increase_number(index, worker_number)
            self.partition_infor[worker_id] = [index, second_index]



            #self.partition_leader[self.partition_infor[worker][0]]=worker

        debug_print_by_name('wentao', str(self.partition_infor))
        self._master.ship_streaming_meta_data(self.job_id, self.partition_infor)

    def choose_leader(self):
        for worker_id in self.partition_infor.keys():
            for partition_id in self.partition_infor[worker_id]:
                if partition_id not in self.partition_leader.keys():
                    self.partition_leader[partition_id] = worker_id

    def check_if_streaming(self, task):
        rdd_iter = task.last_rdd
        while True:
            if not isinstance(rdd_iter, NarrowRDD):
                return isinstance(rdd_iter, Streaming)
            rdd_iter = rdd_iter.parent



    def assign_task(self, task):
        """Assign the stages list to Master Node,
           return the last rdd that action should be applied"""
                # Choose a Leader
        print_infor(bcolors.OKGREEN, "[StreamingDriver] Assigning Task {0}...".format(task.task_id),
                    self._master.debug)

        #print "!!!!!!{0}, {1}".format(self.partition_infor, self.partition_leader)
        worker_id = -1
        if self.check_if_streaming(task):
            partition_id = int(task.task_id.split("_")[1])
            worker_id = self.partition_leader[partition_id]
            task.input_source[0]['interval'] = self.interval
        # if partition_id in self.partition_leader.keys():
        #     worker_id = self.partition_leader[partition_id]
            # worker_info=self._master.worker_list[worker_id]
        # else:
        worker_info = self._master.get_available_worker(worker_id)
        while worker_info is None:
            gevent.sleep(0.5)
            worker_info = self._master.get_available_worker(worker_id)

        unique_task_id = '{job_id}_{task_id}'.format(job_id=self.job_id,
                                                     task_id=task.task_id)
        self.task_node_table[unique_task_id] = worker_info

        ret = self._master.assign_task(worker_info["worker_id"], task,
                                       self.task_node_table)
        if ret == 0:
            self.task_list[task] = "Assigned"
        print_infor(bcolors.OKGREEN, "[StreamingDriver] Assigning Task {0}... Finished".format(
            task.task_id), self._master.debug)
        # print_infor(bcolors.OKGREEN,
        #     "[StreamingDriver] Task {0}.input_resource {1}".format(task.task_id,
        #                                                        task.input_source),
        #     self._master.debug)
        return ret

    def fault_handler(self, worker_id):
        # Repick leader for partitions
        for part_id in self.partition_infor[worker_id]:
            if self.partition_leader[part_id] is worker_id:
                for candidate_id in self.partition_infor.keys():
                    if (candidate_id is not worker_id) and (part_id in self.partition_infor[candidate_id]):
                        self.partition_leader[part_id] = candidate_id
                        break
        self.partition_infor.__delitem__(worker_id)

        print_infor(bcolors.OKGREEN, "[Worker] Worker {0} is down!".format(worker_id))
        task_node_table_keys = filter(
            lambda a: self.task_node_table[a]["worker_id"] == worker_id,
            self.task_node_table.keys())
        task_node_table_keys.sort(
            lambda a, b: cmp(int(a.split('_')[1]), int(b.split('_')[1])))
        task_list = []

        for job_task_id in task_node_table_keys:
            task_id = job_task_id.split('_', 1)[1]
            for task_i in self.task_list.keys():
                if task_i.task_id == task_id:
                    task_list.append(task_i)
                    break

        gevent.spawn(self.assign_task_list, task_list)

    def do_drive(self, last_rdd, action_name, *args):
        self.choose_leader()
        self.action = self.actions[action_name]
        lineage = last_rdd.get_lineage()
        print_infor(bcolors.OKGREEN, "[SparkDriver]*****************", self._master.debug)
        print_infor(bcolors.OKGREEN, "[SparkDriver]*** Partition Form:", self._master.debug)
        for rdd in lineage:
            print_infor(bcolors.OKGREEN, "[SparkDriver]***{0}    {1}".format(rdd[1], rdd[
                0].partitions()), self._master.debug)
        print_infor(bcolors.OKGREEN, "[SparkDriver]*****************", self._master.debug)
        self.args = args
        # generate graph-table and stages
        # partition_graph = self.gen_graph_table(last_rdd)
        self.init_tasks(lineage)
        # Do some fuction to generate the rdd that apply the operation and the result
        tasks = self.task_list.keys()
        tasks.sort(lambda x, y: cmp(int(x.task_id.split("_")[0]),
                                    int(y.task_id.split("_")[0])))
        debug_print_by_name('kaijie', str(tasks))
        # print "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX{0}".format(tasks[0].task_id)
        gevent.spawn(self.assign_task_list, tasks)

    def generate_task_id(self, cur_stage_id, cur_par_id):
        return "{0}_{1}_{2}".format(cur_stage_id, cur_par_id, time.time())