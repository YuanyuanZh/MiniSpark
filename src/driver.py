from src.rdd.rdd import WideRDD, TextFile, GroupByKey, Map, Join
from src.util import util_pickle


class SparkDriver:
    def __init__(self):
        self.actions = {"reduce": self.do_reduce,
                        "collect": self.do_collect,
                        }

    def do_drive(self, serialized_rdd, action_name, func):
        last_rdd = util_pickle.unpickle_object(serialized_rdd)
        lineage = last_rdd.get_lineage()

        # generate graph-table and stages
        partition_graph = self.gen_graph_table(last_rdd)

        self.partitionRdd(lineage, partition_graph)

        stages = self.split_stages(lineage)

        # Do some fuction to generate the rdd that apply the operation and the result
        rdd = self.assign_stages(stages)
        return self.actions[action_name](rdd, func)

    def gen_graph_table(self, rdd):
        """Generate the table of partitons from the rdd"""
        return rdd.partitions()

    def split_stages(self, lineage):
        """Split the lineage into stages,
           return the list of Rdd that represent the stage """
        stages=[]
        prev=lineage[0][0]
        for rdd, rdd_id in lineage:
            if isinstance(rdd, WideRDD):
                stages.append(prev)
            prev = rdd
        stages.append(prev)
        return stages

    def assign_stages(self, stages):
        """Assign the stages list to Master Node,
           return the last rdd that action should be applied"""
        pass

    def partitionRdd(self, lineage, partition_graph):
        """New Partition obj of each RDD """
        # for rdd in lineage:
        #     for()

        pass

    def do_reduce(self, rdd, func):
        pass

    def do_collect(self, rdd, func=None):
        pass


class RDDPartition():
    def __init__(self, pid, parent_rdd, parent_pid):
        self.pid=pid
        self.parent=(parent_rdd, parent_pid)
