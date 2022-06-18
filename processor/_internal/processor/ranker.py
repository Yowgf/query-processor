from common.log import log
from common.memory.defs import MEGABYTE
from common.utils.index import (read_index,
                                read_index_metadata)
from common.utils.url_mapping import read_url_mapping

logger = log.logger()

MAX_READ_CHARS = 256 * MEGABYTE

class TFIDF:
    def __init__(self, index_fpath: str):
        self._index_fpath = index_fpath
        self._checkpoint = 0

    def init(self):
        logger.info("Initializing ranker")

        url_mapping, checkpoint = read_url_mapping(self._index_fpath, 0)

        index_metadata, checkpoint = read_index_metadata(self._index_fpath,
                                                         checkpoint)

        self._url_mapping = url_mapping
        self._num_docs = index_metadata.num_docs
        self._checkpoint = checkpoint

        logger.info("Successfully initialized ranker")

    def train(self):
        logger.info(f"Training ranker from path '{self._index_fpath}'")

        checkpoint = self._checkpoint
        while checkpoint != None:
            index, checkpoint = read_index(self._index_fpath, checkpoint,
                                           MAX_READ_CHARS)
            logger.info(f"Read index. Length: {len(index)}")

        logger.info(f"Successfully trained ranker from path '{self._index_fpath}'")

    def rank(self, query: str):
        logger.info(f"Ranking query: '{query}'")

        

        logger.info(f"Successfully ranked query: '{query}'")

class BM25:
    def __init__(self, index_fpath: str):
        self._index_fpath = index_fpath

    def init(self):
        logger.info("Initializing ranker")

        logger.info("Successfully initialized ranker")


    def train(self):
        logger.info(f"Training ranker from path '{self._index_fpath}'")
        logger.info(f"Successfully trained ranker from path '{self._index_fpath}'")

    def rank(self, query: str):
        logger.info(f"Ranking query: '{query}'")

        

        logger.info(f"Successfully ranked query: '{query}'")

def new_ranker(ranker_type, index_fpath):
    logger.info(f"Creating ranker of type {ranker_type}")
    if ranker_type == "TFIDF":
        return TFIDF(index_fpath)
    elif ranker_type == "BM25":
        return BM25(index_fpath)
    else:
        raise ValueError(f"Invalid ranker type {ranker_type}")
