import json

class Statistics:
    def __init__(self):
        self.index_size = None
        self.num_lists = None
        self.avg_list_size = None
        self.elapsed_time = None

    def set_index_size(self, index_size):
        self.index_size = index_size

    def set_num_lists(self, num_lists):
        self.num_lists = num_lists

    def set_avg_list_size(self, avg_list_size):
        self.avg_list_size = avg_list_size

    def set_elapsed_time(self, elapsed_time):
        self.elapsed_time = elapsed_time

    def to_json(self):
        return json.dumps({
            "Index Size": self.index_size,
            "Number of Lists": self.num_lists,
            "Average List Size": self.avg_list_size,
            "Elapsed Time": self.elapsed_time,
        })
