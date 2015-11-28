import re
import sys
from src.rdd import partition


class RDD(object):
    default = {}
    default['num_partition_RBK'] = 2
    current_id = 0
    narrow = ['Map','FlatMap','Filter']
    wide = ['ReduceByKey','GroupByKey']

    def __init__(self,config = default):
        self.lineage = None
        self.config = config

    def lineage(self):
        pass

    def partitions(self):
        pass

    def partitioner(self):
        pass

    def collect(self):
        elements = []
        while True:
            element = self.get()
            if element == None:
                break
            elements.append(element)
        return elements

    def count(self):
        return len(self.collect())

    def reduce(self):
        pass

    def save(self,path,output_name):
        pass


class TextFile(RDD):

    def __init__(self, filename, partition, num_partitions):
        super(TextFile,self).__init__()
        self.filename = filename
        self.lines = None
        self.index = 0
        self.partition = partition
        self.num_partitions = num_partitions
        self.id = "TextFile"

    def get_lineage(self):
        lineage =[(self,self.id)]
        self.lineage = lineage
        return lineage

    def partitions(self):
        partitions = []
        index = self.lineage.index((self,self.id))
        next_op = self.lineage[index+1]
        if re.sub(r'_[0-9]', "", next_op[1]) in RDD.narrow:
            for i in range(self.num_partitions):
                partitions.append([str(i)])
        else:
            for i in range(self.num_partitions):
                sub = []
                for j in range(next_op[0].num_partitions):
                    sub.append(str(i)+str(j))
                partitions.append(sub)
        return partitions

    def get(self):
        if not self.lines:
            f = open(self.filename)
            self.lines = self.read_file(f)
            f.close()
        # return self.lines
        if self.index == len(self.lines):
            return None
        else:
            line = self.lines[self.index]
            self.index += 1
            return line

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


class Map(RDD):

    def __init__(self, parent, func):
        super(Map,self).__init__()
        self.parent = parent
        self.func = func
        self.id = "Map_{0}".format(Map.current_id)
        Map.current_id += 1
        self.num_partitions = self.parent.num_partitions

    def get_lineage(self):
        lineage = self.parent.get_lineage()
        lineage.append((self,self.id))
        self.lineage = lineage
        return lineage

    def partitions(self):
        partitions = self.parent.partitions()
        index = self.lineage.index((self,self.id))
        next_op = self.lineage[index+1]
        if index+1 < len(self.lineage):
            if re.sub(r'_[0-9]', "", next_op[1]) in RDD.wide:
                new_partitions = []
                for i in range(self.num_partitions):
                    sub = []
                    for j in range(next_op[0].num_partitions):
                        sub.append(str(i)+str(j))
                    new_partitions.append(sub)
                partitions = new_partitions
        return partitions

    def get(self):

        element = self.parent.get()
        if element == None:
            return None
        else:
            element_new = []
            for e in element:
                element_new.append(self.func(re.sub(r'\n', "", e)))
            return element_new


class Filter(RDD):
    
    def __init__(self, parent, func):
        super(Filter,self).__init__()
        self.parent = parent
        self.func = func
        self.id = "Filter_{0}".format(Filter.current_id)
        Filter.current_id += 1
        self.num_partitions = 0

    def get_lineage(self):
        lineage = self.parent.get_lineage()
        lineage.add((self,self.id))
        self.lineage = lineage
        return lineage

    def partitions(self):
        partitions = self.parent.partitions()
        index = self.lineage.index((self,self.id))
        if index+1 < len(self.lineage):
            next_op = self.lineage[index+1]
            if re.sub(r'_[0-9]', "", next_op[1]) in RDD.wide:
                new_partitions = []
                for i in range(self.num_partitions):
                        sub = []
                        for j in range(next_op[0].num_partitions):
                            sub.append(str(i)+str(j))
                        new_partitions.append(str(i))
                partitions = new_partitions
        return partitions

    def get(self):
        while True:
            element = self.parent.get()
            if element == None:
                return None
            else:
                if self.func(element):
                    return element


class FlatMap(RDD):

    def __init__(self, parent, func):
        super(FlatMap,self).__init__()
        self.parent = parent
        self.func = func
        self.id = "FlatMap_{0}".format(FlatMap.current_id)
        FlatMap.current_id +=1
        self.num_partitions = self.parent.num_partitions

    def get_lineage(self):
        lineage = self.parent.get_lineage()
        lineage.append((self,self.id))
        self.lineage = lineage
        return lineage

    def partitions(self):
        partitions = self.parent.partitions()
        index = self.lineage.index((self,self.id))
        if index+1 < len(self.lineage):
            next_op = self.lineage[index+1]
            if re.sub(r'_[0-9]', "", next_op[1]) in RDD.wide:
                new_partitions = []
                for i in range(self.num_partitions):
                        sub = []
                        for j in range(next_op[0].num_partitions):
                            sub.append(str(i)+str(j))
                        new_partitions.append(str(i))
                partitions = new_partitions
        return partitions

    def get(self):
        element = self.parent.get()
        if element == None:
            return None
        else:
            new_element = self.func(element)
            rst = []
            for e in new_element:
                rst.append(e)
            return rst


class Join(RDD):
    def __init__(self, parent):
        super(Join,self).__init__()
        self.parent = parent

class MapValue(RDD):
    def __init__(self, parent,func):
        super(MapValue,self).__init__()
        self.parent = parent
        self.func = func

class GroupByKey(RDD):

    def __init__(self, parent):
        super(GroupByKey,self).__init__()
        self.parent = parent
        self.id = "GroupByKey_{0}".format(GroupByKey.current_id)
        GroupByKey.current_id += 1
        self.num_partitions = 0

    def get_lineage(self):
        lineage = self.parent.get_lineage()
        lineage.append((self,self.id))
        return lineage

    def get(self):
        element = self.parent.get()
        if element == None:
            return None
        else:
            element_new = {}
            for k in element.keys():
                v = element[k]
                if k in element_new:
                    element_new[k].append(v)
                else:
                    element_new[k] = [v]
            return element_new


class ReduceByKey(RDD):

    def __init__(self, parent,func):
        super(ReduceByKey,self).__init__()
        self.parent = parent
        self.id = "ReduceByKey_{0}".format(ReduceByKey.current_id)
        ReduceByKey.current_id += 1
        self.func = func
        self.num_partitions = self.config['num_partition_RBK']

    def get_lineage(self):
        lineage = self.parent.get_lineage()
        lineage.append((self,self.id))
        self.lineage = lineage
        return lineage

    def partitions(self):
        partitions = []
        index = self.lineage.index((self,self.id))
        if index+1 < len(self.lineage):
            next_op = self.lineage[index+1]
            if re.sub(r'_[0-9]', "", next_op[1]) in RDD.wide:
                for i in range(self.num_partitions):
                    sub = []
                    for j in range(next_op[0].num_partitions):
                        sub.append(str(i)+str(j))
                    partitions.append(sub)
        else:
            for i in range(self.num_partitions):
                partitions.append([str(i)])
        return partitions

    def get(self):
        collect = {}
        element = self.parent.get()
        if element == None:
            return None
        else:
            while element != None:
                for e in element:
                    if e[0] in collect:
                        collect[e[0]].append(e[1])
                    else:
                        collect[e[0]] = [e[1]]
                element = self.parent.get()
        rst = []
        for key in collect.keys():
            v = collect.get(key)
            rst.append({key:reduce(self.func,v)})
        return rst

if __name__ == '__main__':
    filename = sys.argv[1]
    # partitions = partition.FilePartition('../../files/wordcount',1).partition()
    # for p in partitions:
    #     # r = TextFile('myfile',partitions.get(p))
    #     # m = Map(r, lambda s: s.split())
    #     # f = Filter(m, lambda a: int(a[1]) > 2)
    #     # print f.collect()
    #     lines = TextFile('../../files/wordcount',partitions.get(p), 1)
    #     f = FlatMap(lines,lambda x: x.split(' '))
    #     m = Map(f,lambda x: (x, 1))
    #     counts = ReduceByKey(m,lambda a, b: a+b)
    #     output = counts.collect()
    #     print(output)
    #     print (counts.get_lineage())
    #     print(lines.partitions())
    #     print (f.partitions())
    #     print(m.partitions())
    #     print(counts.partitions())

    def computeContribs(l, r):
        return l/len(r)

    partitions = partition.FilePartition(filename,1).partition()
    for p in partitions:
        t = TextFile(filename,partitions.get(p))
        links = Map(t,lambda s: s.split())
        ranks = Map(links,lambda url_neighbors: (url_neighbors[0], 1.0))
        for iteration in range(int(sys.argv[2])):
            joins = Join(links, ranks)
            contribs = FlatMap(joins,lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
            ranks = ReduceByKey(contribs,lambda a,b:a+b)
            rst = MapValue(ranks,lambda rank: rank * 0.85 + 0.15)
            collect = rst.collect()
        for (link, rank) in collect:
            print("%s has rank: %s."%(link, rank))




