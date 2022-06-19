import json
from math import log2

from common.log import log
from common.memory.defs import MEGABYTE
from common.utils.index import (read_index,
                                read_index_metadata)
from common.utils.url_mapping import read_url_mapping
from common.preprocessing.normalize import tokenize_and_normalize
from .utils import subindex_with_words
from .score_heap import ScoreHeap

logger = log.logger()

MAX_READ_CHARS = 256 * MEGABYTE
NUM_RESULTS = 10

# TODO: change inheritance scheme of the rankers. However, this should only be
# done if I am sure that the classes will largely preserve the same behavior.
#
# - Can have single class Ranker
# - Ranker.__init__ will set ranker._score = tfidf, bm25
#
#
# TODO: use DAAT instead of TAAT
#

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

        tokens = tokenize_and_normalize(query)

        subindex = subindex_with_words(self._index_fpath, self._checkpoint, tokens)

        scores = ScoreHeap()
        for word in subindex:
            postings = subindex[word]
            new_scores = self._score(word, postings)
            for docid in new_scores:
                scores.push(docid, new_scores[docid])

        results = []
        for _ in range(NUM_RESULTS):
            if len(scores) == 0:
                break

            docid, score = scores.pop()
            while len(scores) > 0:
                new_docid, new_score = scores.pop()
                if new_docid != docid:
                    # Put back
                    scores.push(new_docid, new_score)
                    break
                score += new_score

            results.append({
                "URL": self._url_mapping[docid],
                "Score": score,
            })

        result_json = {}
        result_json["Query"] = query
        result_json["Results"] = results

        print(json.dumps(result_json))

        logger.info(f"Successfully ranked query: '{query}'")

    def _score(self, word, postings):
        scores = {}

        for posting in postings:
            docid, weight = posting
            score = self._tfidf(weight, len(postings))
            scores[docid] = score

        return scores

    def _tfidf(self, freq, len_postings):
        tf = freq
        idf = log2((self._num_docs + 1) / len_postings)
        #logger.info(f"TFIDF for {freq}, {len_postings}: {tf}, {idf}")
        return tf * idf

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
