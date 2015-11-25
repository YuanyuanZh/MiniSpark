import partition


class RDD(object):

    trans_path = []

    def __init__(self):
        pass

    def collect(self):
        elements = []
        while True:
            element = self.get()
            if element == None:
                break
            elements.append(element)
        return elements

    def get_path(self):
        return RDD.trans_path

    def count(self):
        return len(self.collect())

    def reduce(self):
        pass

    def save(self,path,output_name):
        pass


class TextFile(RDD):

    def __init__(self, filename, partition):
        self.filename = filename
        self.lines = None
        self.index = 0
        self.partition = partition
        RDD.trans_path.insert(0,'TextFile')

    def get(self):
        if not self.lines:
            f = open(self.filename)
            self.lines = self.read_file(f)
            f.close()
    
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
        self.parent = parent
        self.func = func
        RDD.trans_path.insert(0,'Map')

    def get(self):

        element = self.parent.get()
        if element == None:
            return None
        else:
            element_new = self.func(element)
            return element_new


class FlatMap(RDD):
    def __init__(self, parent, func):
        self.parent = parent
        self.func = func
        RDD.trans_path.insert(0,'FlatMap')

    def get(self):
        element = self.parent.get()
        if element == None:
            return None
        else:
            element_new = []
            for e in element:
                element_new.append(self.func(element))
            return element_new


class Filter(RDD):
    
    def __init__(self, parent, func):
        self.parent = parent
        self.func = func
        RDD.trans_path.insert(0,'Filter')

    def get(self):
        while True:
            element = self.parent.get()
            if element == None:
                return None
            else:
                if self.func(element):
                    return element

class GroupByKey(RDD):
    def __init__(self, parent):
        self.parent = parent

    def get(self):
        pass


if __name__ == '__main__':

    partitions = partition.FilePartition(['myfile'],1).partition().get('myfile')
    for p in partitions:
        # r = TextFile('myfile',partitions.get(p))
        # m = Map(r, lambda s: s.split())
        # fm = FlatMap(m,lambda s: {s: '1'})
        # f = Filter(m, lambda a: int(a[1]) > 2)
        # print f.collect()
        # print f.get_path()
        r = TextFile('wordcount',partitions.get(p))
        m = Map(r, lambda s: s.split())

        fm = FlatMap(m,lambda s: {s:1})
        # f = Filter(m, lambda a: int(a[1]) > 2)
        print fm.collect()
        print fm.get_path()







