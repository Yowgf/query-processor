from datetime import datetime

from common.log import log
from .ranker import Ranker

logger = log.logger()

class Processor:
    def __init__(self, config):
        self._index_file = config.index_file
        self._queries_file = config.queries
        self._parallelism = config.parallelism
        self._benchmarking = config.benchmarking
        self._ranker = Ranker(config.ranker, self._index_file, self._parallelism)

        self._time_init = None
        self._time_run = None

    def init(self):
        logger.info(f"Initializing query processor")

        before = datetime.now()

        self._queries = open(self._queries_file, "r").read().strip().split("\n")
        self._ranker.init(self._queries)

        self._time_init = (datetime.now() - before).total_seconds()
        logger.info(f"Total time spent initializing: {self._time_init}")

        logger.info(f"Successfully initialized query processor")

    def run(self):
        logger.info("Running query processor")

        before = datetime.now()

        results_json = self._ranker.rank()

        self._time_run = (datetime.now() - before).total_seconds()
        logger.info(f"Total time spent after ranking: {self._time_run}")

        if self._benchmarking:
            # Only print the run time, because in the benchmark we are focused
            # in the speedup gains by parallelizing.
            print(f"{self._time_run:.6f}")
        else:
            for result in results_json:
                print(result)

        logger.info("Successfully ran query processor")
