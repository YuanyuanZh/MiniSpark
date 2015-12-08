from src.util.util_debug import debug_print_by_name


class HashPartition(object):
    def __init__(self, data, num_partitions):
        self.data = data
        self.num_partitions = num_partitions

    def partition(self):
        rst = {}
        debug_print_by_name('wentao', str(self.data))
        for i in range(len(self.data)):
            hashcode = self.hash_func(self.data[i][0])
            if hashcode in rst:
                rst[hashcode].append(self.data[i])
            else:
                rst[hashcode] = [self.data[i]]
        return rst

    def hash_func(self, key):
        return ord(key[0]) % self.num_partitions


class RangePartition(object):
    def __init__(self, filename, split_size):
        self.filename = filename
        self.split_size = split_size

    def partition(self):
        rst = {}
        file_object = open(self.filename)
        file_object.seek(0, 2)
        end = file_object.tell()
        offset = 0
        partition_id = 0
        while offset < end:
            rst[partition_id] = [offset, self.split_size]
            partition_id += 1
            offset += self.split_size
        return rst
