from src.rdd import partition
from src.util.util_debug import debug_print
from src.util.util_zerorpc import get_client, execute_command


class RDD(object):
    _config = {}
    _current_id = 0

    def __init__(self):
        pass

    def lineage(self):
        pass

    def partitions(self):
        pass

    def partitioner(self):
        pass

    def get_lineage(self):
        lineage = self.parent.get_lineage()
        lineage += [(self, self.id)]
        self.lineage = lineage
        return lineage

    def collect(self, driver):
        driver.do_drive(self, "collect")

    def count(self, driver):
        #driver=SparkDriver._master.get_master().produce_driver()
        driver.do_drive(self, "count")

    def reduce(self, driver, func):
        #driver=SparkDriver._master.get_master().produce_driver()
        driver.do_drive(self, "reduce", func)

    def save(self, driver, output_name):
        #driver=SparkDriver._master.get_master().produce_driver()
        driver.do_drive(self,"save", output_name)

    def partition_intermediate_rst(self, data, num_partitions):
        if num_partitions is not None:
            hash_partitioner = partition.HashPartition(data, num_partitions)
            return hash_partitioner.partition()
        return data

class InputRDD(RDD):
    pass


class Streaming(InputRDD):

    def get(self, input_source):
        job_id = input_source['job_id']
        partition_id = input_source['partition_id']
        return input_source['streaming_data'][job_id][partition_id]


class NarrowRDD(RDD):

    def partitions(self):
        partitions = self.parent.partitions()
        self.num_partitions = self.parent.num_partitions
        index = self.lineage.index((self, self.id))
        if index + 1 < len(self.lineage):
            next_op = self.lineage[index + 1]
            if isinstance(next_op[0], WideRDD):
                new_partitions = []
                for i in range(self.num_partitions):
                    sub = []
                    for j in range(next_op[0].num_partitions):
                        self.num_rst_partitions = next_op[0].num_partitions
                        sub.append(str(i) + '_' + str(j))
                    new_partitions.append(sub)
                partitions = new_partitions
        return partitions

    def partitioner(self):
        return 'RangePartition'


class MultiParentNarrowRDD(NarrowRDD):
    """
    Just For Classification
    """
    def __init__(self):
        pass

    def shuffle(self, input_source):
        results = []
        #debug_print("[Wide-RDD] {0} InputSource is {1}".format(self.id, input_source))
        for partitions in input_source:
            debug_print("[Shuffle] {0} Shuffling from source {1}".format(self.id, partitions))
            if not isinstance(partitions, list):
                partitions=[partitions]
            for p in partitions:
                result=None
                #TODO Here May Return None
                while result is None:
                    task_node_table=input_source["task_node_table"]
                    worker_id= task_node_table["{0}_{1}".format(p['job_id'],p['task_id'])]["worker_id"]
                    client = get_client(worker_id)
                    debug_print("[Shuffle] {0} get a None from {1}, at Part {2}, retrying".format(self.id, p['task_id'],p['partition_id']))
                    result=execute_command(client, client.get_rdd_result,
                                          p['job_id'],
                                          p['task_id'],
                                          p['partition_id'])
                debug_print("[Shuffle] {0} get a result={1} from {2}, at Part {3}".format(self.id, result,  p['task_id'],p['partition_id']))
                results += result
        return results


class WideRDD(RDD):

    def partitions(self):
        partitions = []
        index = self.lineage.index((self, self.id))
        if index + 1 < len(self.lineage):
            next_op = self.lineage[index + 1]
            if isinstance(next_op[0], WideRDD):
                for i in range(self.num_partitions):
                    sub = []
                    for j in range(next_op[0].num_partitions):
                        self.num_rst_partitions = next_op[0].num_partitions
                        sub.append(str(i) + '_' + str(j))
                    partitions.append(sub)
            else:
                for i in range(self.num_partitions):
                    partitions.append([str(i) + '_' + str(i)])
        else:
            for i in range(self.num_partitions):
                partitions.append([str(i) + '_' + str(i)])
        return partitions

    def partitioner(self):
        return 'HashPartition'

    def shuffle(self, input_source):
        results = []
        #debug_print("[Wide-RDD] {0} InputSource is {1}".format(self.id, input_source))
        for partitions in input_source:
            debug_print("[Shuffle] {0} Shuffling from source {1}".format(self.id, partitions))
            if not isinstance(partitions, list):
                partitions=[partitions]
            for p in partitions:
                result=None
                #TODO Here May Return None
                while result is None:
                    task_node_table=input_source["task_node_table"]
                    worker_id= task_node_table["{0}_{1}".format(p['job_id'],p['task_id'])]["worker_id"]
                    client = get_client(worker_id)
                    debug_print("[Shuffle] {0} get a None from {1}, at Part {2}, retrying".format(self.id, p['task_id'],p['partition_id']))
                    result=execute_command(client, client.get_rdd_result,
                                          p['job_id'],
                                          p['task_id'],
                                          p['partition_id'])
                debug_print("[Shuffle] {0} get a result={1} from {2}, at Part {3}".format(self.id, result,  p['task_id'],p['partition_id']))
                results += result
        return results


class TextFile(InputRDD):
    def __init__(self, filename):
        self.filename = filename
        self.id = "TextFile_{0}".format(TextFile._current_id)
        TextFile._current_id += 1
        self.data = None
        self.lineage = None
        self.file_split_info = partition.RangePartition(self.filename, RDD._config['split_size']).partition()
        self.num_partitions = len(self.file_split_info.values())
        self.num_rst_partitions = None

    def get_lineage(self):
        lineage = [(self, self.id)]
        self.lineage = lineage
        return lineage

    def partitions(self):
        partitions = []
        index = self.lineage.index((self, self.id))
        next_op = self.lineage[index + 1]
        if isinstance(next_op[0], WideRDD):
            for i in range(self.num_partitions):
                sub = []
                for j in range(next_op[0].num_partitions):
                    self.num_rst_partitions = next_op[0].num_partitions
                    sub.append(str(i) + '_' + str(j))
                partitions.append(sub)
        else:
            for i in range(self.num_partitions):
                partitions.append([str(i) + '_' + str(i)])
        return partitions

    def get(self, input_source):
        debug_print("[TextFile-RDD] id={0} InputSource is {1}, data={2}".format(self.id, input_source, self.data))
        partition_id = input_source
        if not self.data:
            f = open(self.filename)
            self.data = self.read_file(f, self.file_split_info[int(partition_id)])
            f.close()
        self.data = self.partition_intermediate_rst(self.data, self.num_rst_partitions)
        return self.data

    def read_file(self, f, start_end):
        f.seek(0, 2)
        end = f.tell()
        offset = start_end[0]
        size = start_end[1]
        f.seek(offset)
        rst = []
        if offset == 0:
            rst.append(f.readline())
        else:
            f.readline()
        pos = f.tell()
        while pos < end and pos <= offset + size:
            line = f.readline()
            rst.append(line)
            pos = f.tell()
        return rst


class Map(NarrowRDD):
    def __init__(self, parent, func):
        self.parent = parent
        self.func = func
        self.id = "Map_{0}".format(Map._current_id)
        Map._current_id += 1
        self.num_partitions = self.parent.num_partitions
        self.num_rst_partitions = None
        self.data = None
        self.lineage = None

    def get(self, input_source):
        debug_print("[Map-RDD] InputSource is {0}".format(input_source))

        if not self.data:
            element = self.parent.get(input_source)
            if element == None:
                return None
            else:
                element_new = []
            for e in element:
                element_new.append(self.func(e))
                self.data = element_new
        self.data = self.partition_intermediate_rst(self.data, self.num_rst_partitions)
        return self.data


class Filter(NarrowRDD):
    def __init__(self, parent, func):
        self.parent = parent
        self.func = func
        self.id = "Filter_{0}".format(Filter._current_id)
        Filter._current_id += 1
        self.num_partitions = self.parent.num_partitions
        self.num_rst_partitions = None
        self.data = None
        self.lineage = None

    def get(self, input_source):
        debug_print("[Filter-RDD] InputSource is {0}".format(input_source))

        if not self.data:
            elements = self.parent.get(input_source)
            if elements == None:
                return None
            else:
                rst = []
                for e in elements:
                    if self.func(e):
                        rst.append(e)
                self.data = rst
        self.data = self.partition_intermediate_rst(self.data, self.num_rst_partitions)
        return self.data


class FlatMap(NarrowRDD):
    def __init__(self, parent, func):
        self.parent = parent
        self.func = func
        self.id = "FlatMap_{0}".format(FlatMap._current_id)
        FlatMap._current_id += 1
        self.num_partitions = self.parent.num_partitions
        self.num_rst_partitions = None
        self.data = None
        self.lineage = None

    def get(self, input_source):
        debug_print("[FlatMap-RDD] InputSource is {0}".format(input_source))
        if not self.data:
            element = self.parent.get(input_source)
            if element == None:
                return None
            else:
                new_element = []
                for e in element:
                    rst = self.func(e)
                    new_element += rst
                self.data = new_element
        self.data = self.partition_intermediate_rst(self.data, self.num_rst_partitions)
        debug_print("[FlatMap-RDD] {0} has destnation# {1}, now data is {2}".format(self.id, self.num_rst_partitions,self.data))
        return self.data


class Join(MultiParentNarrowRDD):
    def __init__(self, parent):
        self.parent = parent
        self.id = "Join_{0}".format(Join._current_id)
        Join._current_id += 1
        self.num_partitions = self.parent[0].num_partitions
        self.num_rst_partitions = None
        self.data = None
        self.lineage = None

    def get_lineage(self):
        lineage = []
        for p in self.parent:
            lineage += p.get_lineage()
        lineage += [(self, self.id)]
        self.lineage = lineage
        return lineage

    def get(self, input_source):
        debug_print("[Join-RDD] {0} has Parnets {1}".format(self.id, map(lambda x: x.id, self.parent)))
        if not self.data:
            elements = self.shuffle(input_source)
            element1 = elements[0]
            element2 = elements[1]
            # element1 = self.parent[0].get([input_source])
            # element2 = self.parent[1].get([input_source])
            if element1 == None or element2 == None:
                return None
            else:
                rst = []
                for i in range(len(element1)):
                    rst.append((element1[i][0], (element1[i][1], element2[i][1])))
                self.data = rst
        self.data = self.partition_intermediate_rst(self.data, self.num_rst_partitions)
        debug_print("[Join-RDD] id: {0} self.input_source={1} self.data={2}".format(self.id, input_source, self.data))
        return self.data

    def partitions(self):
        partitions = self.parent[0].partitions()
        self.num_partitions = self.parent[0].num_partitions
        index = self.lineage.index((self, self.id))
        if index + 1 < len(self.lineage):
            next_op = self.lineage[index + 1]
            if isinstance(next_op[0], WideRDD):
                new_partitions = []
                for i in range(self.num_partitions):
                    sub = []
                    for j in range(next_op[0].num_partitions):
                        self.num_rst_partitions = next_op[0].num_partitions
                        sub.append(str(i) + '_' + str(j))
                    new_partitions.append(sub)
                partitions = new_partitions
        return partitions


class MapValue(NarrowRDD):
    def __init__(self, parent, func):
        self.parent = parent
        self.func = func
        self.id = "MapValue_{0}".format(MapValue._current_id)
        MapValue._current_id += 1
        self.num_partitions = self.parent.num_partitions
        self.num_rst_partitions = None
        self.data = None
        self.lineage = None

    def get(self, input_source):
        if not self.data:
            element = self.parent.get(input_source)
            if element == None:
                return None
            else:
                debug_print("[MapValue-RDD] id={0} input_source={1}, element={2}".format(self.id, input_source, element))
                rst = []
                for e in element:
                    rst.append((e[0], self.func(e[1])))
                self.data = rst
        self.data = self.partition_intermediate_rst(self.data, self.num_rst_partitions)
        return self.data


class GroupByKey(WideRDD):
    def __init__(self, parent):
        self.parent = parent
        self.id = "GroupByKey_{0}".format(GroupByKey._current_id)
        GroupByKey._current_id += 1
        self.num_partitions = RDD._config['num_partition_GBK']
        self.num_rst_partitions = None
        self.data = None
        self.lineage = None

    def get(self, input_source):
        if not self.data:
            debug_print("[GroupByKey-RDD]id={0} Data is {1}, need to generate ".format(self.id, self.data))

            # element = self.parent.get(input_source)
            element = self.shuffle(input_source)
            if element == None:
                return None
            else:
                element_new = {}
                debug_print("[GroupByKey-RDD]id={0} input_source={1}, element={2} parents={3}".format(self.id, input_source, element, self.parent.id))
                for e in element:
                    k = e[0]
                    v = e[1]
                    if k in element_new:
                        element_new[k].append(v)
                    else:
                        element_new[k] = [v]
                rst = []
                for key in element_new.keys():
                    rst.append((key, element_new.get(key)))
                self.data = rst
            self.data = self.partition_intermediate_rst(self.data, self.num_rst_partitions)
        debug_print("[GroupByKey-RDD] id: {0} self.data={1}".format(self.id, self.data))
        return self.data


class ReduceByKey(WideRDD):
    def __init__(self, parent, func):
        self.parent = parent
        self.id = "ReduceByKey_{0}".format(ReduceByKey._current_id)
        ReduceByKey._current_id += 1
        self.func = func
        self.num_partitions = RDD._config['num_partition_RBK']
        self.num_rst_partitions = None
        self.data = None
        self.lineage = None

    def get(self, input_source):
        if not self.data:
            collect = {}
            # element = self.parent.get(input_source)
            element = self.shuffle(input_source)
            if element == None:
                return None
            else:
                debug_print("[ReduceByKey-RDD]id={0} input_source={1}, element={2}".format(self.id, input_source, element))
                for e in element:
                    if e[0] in collect:
                        collect[e[0]].append(e[1])
                    else:
                        collect[e[0]] = [e[1]]
            rst = []
            for key in collect.keys():
                v = collect.get(key)
                rst.append((key, reduce(self.func, v)))
            self.data = rst
        self.data = self.partition_intermediate_rst(self.data, self.num_rst_partitions)
        return self.data
