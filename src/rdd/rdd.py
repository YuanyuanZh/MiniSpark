from src.rdd import partition

class RDD(object):
    default = {}
    default['num_partition_RBK'] = 2
    default['num_partition_GBK'] = 2
    current_id = 0

    def __init__(self, config = default):
        self.lineage = None
        self.config = config

    def lineage(self):
        pass

    def partitions(self):
        pass

    def partitioner(self):
        pass

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

    def reduce(self):
        pass

    def save(self,path,output_name):
        pass

    def get_data(self,address,key):
        #todo ZeroRPC
        if len(key) == 1:
            return self.get()
        else:
            data = self.get()
            hash_partitioner = partition.HashPartition(data,len(self.partitions()[0]))
            partitioned_date = hash_partitioner.partition()
            return partitioned_date[int(key[1])]


class NarrowRDD(RDD):
    def __int__(self,parent):
        super(NarrowRDD,self).__init__()
        self.parent = parent

    def get_lineage(self):
        lineage = self.parent.get_lineage()
        lineage.append((self,self.id))
        self.lineage = lineage
        return lineage

    def partitions(self):
        partitions = self.parent.partitions()
        self.num_partitions = self.parent.num_partitions
        index = self.lineage.index((self,self.id))
        if index+1 < len(self.lineage):
            next_op = self.lineage[index+1]
            if isinstance(next_op[0], WideRDD):
                new_partitions = []
                for i in range(self.num_partitions):
                    sub = []
                    for j in range(next_op[0].num_partitions):
                        sub.append(str(i)+'_'+str(j))
                    new_partitions.append(sub)
                partitions = new_partitions
        return partitions

    def partitioner(self):
        return 'RangePartition'


class WideRDD(RDD):
    def __int__(self):
        super(WideRDD,self).__init__()
        # self.parent = parent

    def partitions(self):
        partitions = []
        index = self.lineage.index((self,self.id))
        if index+1 < len(self.lineage):
            next_op = self.lineage[index+1]
            if isinstance(next_op[0], WideRDD):
                for i in range(self.num_partitions):
                    sub = []
                    for j in range(next_op[0].num_partitions):
                        sub.append(str(i)+'_'+str(j))
                    partitions.append(sub)
            else:
                for i in range(self.num_partitions):
                    partitions.append([str(i)+'_'+str(i)])
        else:
            for i in range(self.num_partitions):
                partitions.append([str(i)+'_'+str(i)])
        return partitions

    def partitioner(self):
        return 'HashPartition'


class TextFile(RDD):

    def __init__(self, filename, partition, num_partitions):
        super(TextFile,self).__init__()
        self.filename = filename
        # self.index = 0
        self.partition = partition
        self.num_partitions = num_partitions
        self.id = "TextFile"
        self.data = None

    def get_lineage(self):
        lineage =[(self,self.id)]
        self.lineage = lineage
        return lineage

    def partitions(self):
        partitions = []
        index = self.lineage.index((self,self.id))
        next_op = self.lineage[index+1]
        if isinstance(next_op[0], NarrowRDD):
            for i in range(self.num_partitions):
                partitions.append([str(i)+'_'+str(i)])
        else:
            for i in range(self.num_partitions):
                sub = []
                for j in range(next_op[0].num_partitions):
                    sub.append(str(i)+'_'+str(j))
                partitions.append(sub)
        return partitions

    def get(self):
        if not self.data:
            f = open(self.filename)
            self.data = self.read_file(f)
            f.close()
        return self.data
        # if self.index == len(self.lines):
        #     return None
        # else:
        #     line = self.lines[self.index]
        #     self.index += 1
        #     return line

    def read_file(self, f):
        f.seek(0, 2)
        end = f.tell()
        offset = self.partition[0]
        size = self.partition[1]
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
        super(Map,self).__init__()
        self.parent = parent
        self.func = func
        self.id = "Map_{0}".format(Map.current_id)
        Map.current_id += 1
        self.num_partitions = self.parent.num_partitions
        self.data = []

    def get(self):
        if not self.data:
            element = self.parent.get()
            if element == None:
                return None
            else:
                element_new = []
            for e in element:
                element_new.append(self.func(e))
                self.data = element_new
        return self.data


class Filter(NarrowRDD):
    
    def __init__(self, parent, func):
        super(Filter,self).__init__()
        self.parent = parent
        self.func = func
        self.id = "Filter_{0}".format(Filter.current_id)
        Filter.current_id += 1
        self.num_partitions = self.parent.num_partitions
        self.data = None

    def get(self):
        while True:
            element = self.parent.get()
            if element == None:
                return None
            else:
                if self.func(element):
                    return element


class FlatMap(NarrowRDD):

    def __init__(self, parent, func):
        super(FlatMap,self).__init__()
        self.parent = parent
        self.func = func
        self.id = "FlatMap_{0}".format(FlatMap.current_id)
        FlatMap.current_id +=1
        self.num_partitions = self.parent.num_partitions
        self.data = None

    def get(self):
        if not self.data:
            element = self.parent.get()
            if element == None:
                return None
            else:
                new_element = []
                for e in element:
                    rst = self.func(e)
                    new_element += rst
                self.data = new_element
        return self.data


class Join(NarrowRDD):
    def __init__(self, parent):
        super(Join,self).__init__()
        self.parent = parent
        self.id = "Join_{0}".format(Join.current_id)
        Join.current_id += 1
        self.num_partitions = 1
        self.data = None

    def get_lineage(self):
        lineage = []
        for p in self.parent:
            lineage.append(p.get_lineage())
        lineage.append((self,self.id))
        self.lineage = lineage
        return lineage

    def get(self):
        if not self.data:
            element1 = self.parent[0].get()
            element2 = self.parent[1].get()
            if element1 == None or element2 == None:
                return None
            else:
                rst = []
                for i in range(len(element1)):
                    rst.append((element1[i][0],(element1[i][1],element2[i][1])))
                self.data = rst
        return self.data

    def partitions(self):
        partitions = self.parent[0].partitions()
        self.num_partitions = self.parent[0].num_partitions
        index = self.lineage.index((self,self.id))
        if index+1 < len(self.lineage):
            next_op = self.lineage[index+1]
            if isinstance(next_op[0], WideRDD):
                new_partitions = []
                for i in range(self.num_partitions):
                    sub = []
                    for j in range(next_op[0].num_partitions):
                        sub.append(str(i)+'_'+str(j))
                    new_partitions.append(sub)
                partitions = new_partitions
        return partitions


class MapValue(NarrowRDD):
    def __init__(self, parent,func):
        super(MapValue,self).__init__()
        self.parent = parent
        self.func = func
        self.id = "MapValue_{0}".format(MapValue.current_id)
        MapValue.current_id += 1
        self.data = None

    def get(self):
        if not self.data:
            element = self.parent.get()
            if element == None:
                return None
            else:
                rst = []
                for e in element:
                    rst.append((e[0],self.func(e[1])))
                self.data = rst
        return self.data


class GroupByKey(WideRDD):

    def __init__(self, parent):
        super(GroupByKey,self).__init__()
        self.parent = parent
        self.id = "GroupByKey_{0}".format(GroupByKey.current_id)
        GroupByKey.current_id += 1
        self.num_partitions = self.config['num_partition_GBK']
        self.data = None

    def get_lineage(self):
        lineage = self.parent.get_lineage()
        lineage.append((self,self.id))
        self.lineage = lineage
        return lineage

    def get(self):
        if not self.data:
            element = self.parent.get()
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
                    rst.append((key,element_new.get(key)))
                self.data = rst
        return self.data


class ReduceByKey(WideRDD):

    def __init__(self, parent,func):
        super(ReduceByKey,self).__init__()
        self.parent = parent
        self.id = "ReduceByKey_{0}".format(ReduceByKey.current_id)
        ReduceByKey.current_id += 1
        self.func = func
        self.num_partitions = self.config['num_partition_RBK']
        self.data = None

    def get_lineage(self):
        lineage = self.parent.get_lineage()
        lineage.append((self,self.id))
        self.lineage = lineage
        return lineage

    def get(self):
        if not self.data:
            collect = {}
            element = self.parent.get()
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
                rst.append((key, reduce(self.func,v)))
            self.data = rst
        return self.data





