class Partition(object):
    def __init__(self):
        pass

    def partition(self):
        pass


class HashPartition(Partition):
    def partition(self):
        pass


class RangePartition(Partition):
    def partition(self):
        pass


class FilePartition(Partition):

    def __init__(self,filename,num_partition):
        super(FilePartition,self).__init__()
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


