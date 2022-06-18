from common.log import log

logger = log.logger()

class TFIDF:
    def __init__(self):
        pass

    def rank(self, query: str):
        logger.info(f"Ranking query: '{query}'")

        

        logger.info(f"Successfully ranked query: '{query}'")

class BM25:
    def __init__(self):
        pass

    def rank(self, query: str):
        logger.info(f"Ranking query: '{query}'")

        

        logger.info(f"Successfully ranked query: '{query}'")

def new_ranker(ranker_type):
    logger.info(f"Creating ranker of type {ranker_type}")
    if ranker_type == "TFIDF":
        return TFIDF()
    elif ranker_type == "BM25":
        return BM25()
    else:
        raise ValueError(f"Invalid ranker type {ranker_type}")
