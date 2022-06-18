from common.log import log
from .ranker import new_ranker

logger = log.logger()

class Processor:
    def __init__(self, config):
        self._index_file = config.index_file
        self._queries_file = config.queries
        self._ranker = new_ranker(config.ranker)

    def init(self):
        logger.info(f"Initializing query processor")

        self._queries = open(self._queries_file, "r").read().split("\n")

        logger.info(f"Successfully initialized query processor")

    def run(self):
        logger.info("Running query processor")

        for query in self._queries:
            print(self._ranker.rank(query))

        logger.info("Successfully ran query processor")
