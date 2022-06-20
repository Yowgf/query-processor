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

        self._total_time_spent = 0.0

    def init(self):
        logger.info(f"Initializing query processor")

        before = datetime.now()

        self._queries = open(self._queries_file, "r").read().strip().split("\n")
        self._ranker.init(self._queries)

        self._total_time_spent += (datetime.now() - before).total_seconds()
        logger.info("Total time spent after initializing: {self._total_time_spent}")

        logger.info(f"Successfully initialized query processor")

    def run(self):
        logger.info("Running query processor")

        before = datetime.now()

        results_json = self._ranker.rank()

        self._total_time_spent += (datetime.now() - before).total_seconds()
        logger.info("Total time spent after ranking: {self._total_time_spent}")

        if self._benchmarking:
            print(f"{self._total_time_spent:.6f}")
        else:
            for result in results_json:
                print(result)

        logger.info("Successfully ran query processor")
