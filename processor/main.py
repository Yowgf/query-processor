from common.log import log
from ._internal.processor.processor import Processor

logger = log.logger()

def main(args):
    logger.info("Starting query processor run")

    processor = Processor(args)
    processor.init()
    processor.run()

    logger.info("Successfully finished query processor run")
