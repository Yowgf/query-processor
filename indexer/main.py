from common.log import log
from ._internal.indexer.indexer import Indexer

logger = log.logger()

def main(args):
    logger.info("Starting indexer run")

    indexer = Indexer(args)
    indexer.init()
    indexer.run()

    logger.info("Successfully finished indexer run")
