class Partition(object):
    def __init__(self):
        pass

    def partition(self):
        pass


class HashPartition(object):
    def __init__(self, data, num_partitions):
        super(HashPartition,self).__init__()
        self.data = data
        self.num_partitions = num_partitions

    def partition(self):
        rst = {}
        for i in range(len(self.data)):
            hashcode = self.hash_func(self.data[i][0])
            if hashcode in rst:
                rst[hashcode].append(self.data[i])
            else:
                rst[hashcode] = [self.data[i]]
        return rst

    def hash_func(self,key):
        return ord(key[0])%self.num_partitions


class RangePartition(Partition):

    def __init__(self,filename,num_partition):
        super(RangePartition,self).__init__()
        self.filename = filename
        self.num_partition = num_partition

    def partition(self):
        rst = {}
        file_object = open(self.filename)
        file_object.seek(0,2)
        end = file_object.tell()
        partition_size = end / self.num_partition
        offset = 0
        partition_id = 0
        while offset < end:
            rst[partition_id] = [offset,partition_size]
            partition_id += 1
            offset += partition_size
        return rst


