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

    def __init__(self,files,num_partition):
        self.files = files
        self.num_partition = num_partition

    def partition(self):
        rst = {}
        for f in self.files:
            file_object = open(f)
            file_object.seek(0,2)
            end = file_object.tell()
            sub_rst = {}
            # partitions = 2*self.num_workers
            partition_size = end / self.num_partition
            offset = 0
            partition_id = 0
            while offset < end:
                sub_rst[partition_id] = [offset,partition_size]
                partition_id += 1
                offset += partition_size

            rst[f] = sub_rst
        return rst


