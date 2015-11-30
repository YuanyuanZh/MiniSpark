import zerorpc
from src.rdd import partition
from src.util import util_pickle
from src.util.util_zerorpc import get_client, execute_command


class RDD(object):
    _config = {}
    _current_id = 0

    def __init__(self):
        self.lineage = None

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

    def collect(self):
        # elements = []
        # while True:
        element = self.get()
        if element == None:
            return None
        # elements.append(element)
        return element

    def count(self):
        return len(self.collect())

    def reduce(self, func):
        client = zerorpc.Client()
        driver = client.connect(self._config["driver_addr"])
        seialized_rdd = util_pickle.pickle_object(self)
        return driver.do_drive(seialized_rdd)

    def save(self, path, output_name):
        pass

    def partition_intermediate_rst(self, data, num_partitions):
        if num_partitions is not None:
            hash_partitioner = partition.HashPartition(data, num_partitions)
            return hash_partitioner.partition()
        return data


class Streaming(RDD):
    def __init__(self):
        super(Streaming, self).__init__()

    def get(self, input_source):
        job_id = input_source['job_id']
        partition_id = input_source['partition_id']
        return input_source['streaming_data'][job_id][partition_id]


class NarrowRDD(RDD):
    def __int__(self, parent):
        super(NarrowRDD, self).__init__()
        self.parent = parent

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


class WideRDD(RDD):
    def __int__(self):
        super(WideRDD, self).__init__()

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
        result = []
        for partition in input_source:
            client = get_client(partition['worker_addr'])
            result += execute_command(client, client.get_rdd_result,
                                      partition['parent_id'],
                                      partition['partition_id'])
        return result


class TextFile(RDD):
    def __init__(self, filename):
        super(TextFile, self).__init__()
        self.filename = filename
        self.id = "TextFile_{0}".format(TextFile._current_id)
        TextFile._current_id += 1
        self.data = None
        self.file_split_info = partition.RangePartition(self.filename, self._config['split_size']).partition()
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
        partition_id = input_source[0]['partition_id']
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
        super(Map, self).__init__()
        self.parent = parent
        self.func = func
        self.id = "Map_{0}".format(Map._current_id)
        Map._current_id += 1
        self.num_partitions = self.parent.num_partitions
        self.num_rst_partitions = None
        self.data = None

    def get(self, input_source):
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
        super(Filter, self).__init__()
        self.parent = parent
        self.func = func
        self.id = "Filter_{0}".format(Filter._current_id)
        Filter._current_id += 1
        self.num_partitions = self.parent.num_partitions
        self.num_rst_partitions = None
        self.data = None

    def get(self, input_source):
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
        super(FlatMap, self).__init__()
        self.parent = parent
        self.func = func
        self.id = "FlatMap_{0}".format(FlatMap._current_id)
        FlatMap._current_id += 1
        self.num_partitions = self.parent.num_partitions
        self.num_rst_partitions = None
        self.data = None

    def get(self, input_source):
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
        return self.data


class Join(NarrowRDD):
    def __init__(self, parent):
        super(Join, self).__init__()
        self.parent = parent
        self.id = "Join_{0}".format(Join._current_id)
        Join._current_id += 1
        self.num_partitions = self.parent[0].num_partitions
        self.num_rst_partitions = None
        self.data = None

    def get_lineage(self):
        lineage = []
        for p in self.parent:
            lineage += p.get_lineage()
        lineage += [(self, self.id)]
        self.lineage = lineage
        return lineage

    def get(self, input_source):
        if not self.data:
            element1 = self.parent[0].get([input_source[0]])
            element2 = self.parent[1].get([input_source[1]])
            if element1 == None or element2 == None:
                return None
            else:
                rst = []
                for i in range(len(element1)):
                    rst.append((element1[i][0], (element1[i][1], element2[i][1])))
                self.data = rst
        self.data = self.partition_intermediate_rst(self.data, self.num_rst_partitions)
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
        super(MapValue, self).__init__()
        self.parent = parent
        self.func = func
        self.id = "MapValue_{0}".format(MapValue._current_id)
        MapValue._current_id += 1
        self.num_partitions = self.parent.num_partitions
        self.num_rst_partitions = None
        self.data = None

    def get(self, input_source):
        if not self.data:
            element = self.parent.get(input_source)
            if element == None:
                return None
            else:
                rst = []
                for e in element:
                    rst.append((e[0], self.func(e[1])))
                self.data = rst
        self.data = self.partition_intermediate_rst(self.data, self.num_rst_partitions)
        return self.data


class GroupByKey(WideRDD):
    def __init__(self, parent):
        super(GroupByKey, self).__init__()
        self.parent = parent
        self.id = "GroupByKey_{0}".format(GroupByKey._current_id)
        GroupByKey._current_id += 1
        self.num_partitions = self._config['num_partition_GBK']
        self.num_rst_partitions = None
        self.data = None

    def get(self, input_source):
        if not self.data:
            # element = self.parent.get(input_source)
            element = self.shuffle(input_source)
            if element == None:
                return None
            else:
                element_new = {}
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
        return self.data


class ReduceByKey(WideRDD):
    def __init__(self, parent, func):
        super(ReduceByKey, self).__init__()
        self.parent = parent
        self.id = "ReduceByKey_{0}".format(ReduceByKey._current_id)
        ReduceByKey._current_id += 1
        self.func = func
        self.num_partitions = self._config['num_partition_RBK']
        self.num_rst_partitions = None
        self.data = None

    def get(self, input_source):
        if not self.data:
            collect = {}
            # element = self.parent.get(input_source)
            element = self.shuffle(input_source)
            if element == None:
                return None
            else:
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
