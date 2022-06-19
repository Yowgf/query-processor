from common.log import log
from .ranker import Ranker

logger = log.logger()

class Processor:
    def __init__(self, config):
        self._index_file = config.index_file
        self._queries_file = config.queries
        self._ranker = Ranker(config.ranker, self._index_file)

    def init(self):
        logger.info(f"Initializing query processor")

        self._queries = open(self._queries_file, "r").read().strip().split("\n")
        self._ranker.init()

        logger.info(f"Successfully initialized query processor")

    def run(self):
        logger.info("Running query processor")

        for query in self._queries:
            print(self._ranker.rank(query))

        logger.info("Successfully ran query processor")
