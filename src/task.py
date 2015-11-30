class Task(object):
    def __init__(self, last_rdd, input_source, task_id):
        """
        :param last_rdd:
        :param input_source:
        :param task_id: i_j (stage i, partition j)
        :return:
        """
        self.worker = None
        self.last_rdd = last_rdd
        self.input_source = input_source
        self.task_id = task_id
        self.status = None


    def set_input_source(self, input_source):
        self.input_source = input_source


    def execute(self):
        pass