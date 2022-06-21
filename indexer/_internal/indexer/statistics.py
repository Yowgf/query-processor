import json

class Statistics:
    def __init__(self):
        self.index_size = None
        self.num_lists = None
        self.avg_list_size = None
        self.elapsed_time = None
        self.num_docs = None
        self.num_tokens = None
        self.posting_lens = None

    def set_index_size(self, index_size):
        self.index_size = index_size

    def set_num_lists(self, num_lists):
        self.num_lists = num_lists

    def set_avg_list_size(self, avg_list_size):
        self.avg_list_size = avg_list_size

    def set_elapsed_time(self, elapsed_time):
        self.elapsed_time = elapsed_time

    def set_num_docs(self, num_docs):
        self.num_docs = num_docs

    def set_num_tokens(self, num_tokens):
        self.num_tokens = num_tokens

    def set_posting_lens(self, posting_lens):
        self.posting_lens = posting_lens

    def to_json(self, extra_statistics=False):
        statistics_map = {
            "Index Size": self.index_size,
            "Number of Lists": self.num_lists,
            "Average List Size": self.avg_list_size,
            "Elapsed Time": self.elapsed_time,
        }

        if extra_statistics:
            statistics_map["Number of documents"] = self.num_docs
            statistics_map["Number of tokens"] = self.num_tokens
            statistics_map["Distribution of posting lengths"] = self.posting_lens

        return json.dumps(statistics_map)
