from datetime import datetime
import json
from math import log as natural_log

from common.log import log
from common.memory.defs import MEGABYTE
from common.utils.index import (read_index,
                                read_index_metadata)
from common.utils.url_mapping import read_url_mapping
from common.preprocessing.normalize import tokenize_and_normalize
from .utils import subindex_with_words
from .score_heap import ScoreHeap

logger = log.logger()

MAX_READ_CHARS    = 256 * MEGABYTE
NUM_RESULTS       = 10
RANKER_TYPE_TFIDF = "TFIDF"
RANKER_TYPE_BM25  = "BM25"

class Ranker:
    def __init__(self, ranker_type: str, index_fpath: str):
        self._index_fpath = index_fpath
        self._checkpoint = 0

        if ranker_type not in [RANKER_TYPE_TFIDF, RANKER_TYPE_BM25]:
            raise ValueError(f"Invalid ranker type {ranker_type}")
        self._ranker_type = ranker_type

        # k1 in [1.2, 2.0]
        self._bm25_k1 = 1.5
        self._bm25_b = 0.75

    def init(self, queries):
        logger.info("Initializing ranker")

        before = datetime.now()

        url_mapping, checkpoint = read_url_mapping(self._index_fpath, 0)

        index_metadata, checkpoint = read_index_metadata(self._index_fpath,
                                                         checkpoint)

        self._url_mapping = url_mapping
        self._num_docs = index_metadata.num_docs
        self._max_docid = index_metadata.max_docid
        self._avg_doc_len = index_metadata.avg_doc_len
        self._checkpoint = checkpoint

        self._tokens = {}
        self._all_tokens = []
        for query in queries:
            tokenized_query = tokenize_and_normalize(query)
            self._tokens[query] = tokenized_query
            self._all_tokens.extend(tokenized_query)
        self._subindex = subindex_with_words(self._index_fpath, self._checkpoint,
                                             self._all_tokens)

        elapsed = datetime.now() - before
        logger.info(f"Time spent initializing (seconds): {elapsed.total_seconds()}")

        logger.info("Successfully initialized ranker")

    # rank uses internally stored queries, initialized in the init() function.
    def rank(self):
        logger.info(f"Ranking queries: {list(self._tokens.keys())}")

        before = datetime.now()

        results = []
        for query in self._tokens:
            logger.info(f"Ranking query: '{query}'")

            scores = self._score(self._subindex, self._tokens[query])

            result_json = self._top10_json(query, scores)

            results.append(json.dumps(result_json, ensure_ascii=False))

            logger.info(f"Successfully ranked query: '{query}'")

        elapsed = datetime.now() - before
        logger.info(f"Time spent ranking (seconds): {elapsed.total_seconds()}")

        logger.info(f"Successfully ranked queries: {list(self._tokens.keys())}")

        return results

    # Scores documents in a Document at a time (DAAT) fashion.
    def _score(self, subindex, tokens):
        logger.info(f"Scoring tokens {tokens} with subindex of length: "+
                    f"{len(subindex)}")

        # DAAT adapted from 2022-01 Information Retrieval class slides
        #
        scores = ScoreHeap()
        posting_idxs = {word: 0 for word in subindex}
        for target_docid in range(self._max_docid):
            if target_docid % 10000 == 0:
                logger.debug(f"Scoring document with ID {target_docid}")

            score = 0
            for term in tokens:
                posting_idx = posting_idxs[term]

                while posting_idx < len(subindex[term]):
                    docid, weight = subindex[term][posting_idx]

                    if docid > target_docid:
                        # The inverted list is ordered by docid
                        break
                    elif docid == target_docid:
                        if self._ranker_type == RANKER_TYPE_TFIDF:
                            score += self._tfidf(docid, weight, len(subindex[term]))
                        elif self._ranker_type == RANKER_TYPE_BM25:
                            score += self._bm25(docid, weight, len(subindex[term]))
                        scores.push(docid, score)
                        posting_idx += 1
                        break
                    posting_idx += 1

                posting_idxs[term] = posting_idx

            if score != 0:
                scores.push(docid, score)

        logger.info(f"Successfully scored {tokens} with subindex len "+
                    "{len(subindex)}. Scores length: {len(scores)}")

        return scores

    def _tf(self, docid, freq):
        return freq / self._url_mapping.get_doc_len(docid)

    def _idf(self, len_postings):
        return natural_log((self._num_docs - len_postings + 0.5) / 
                           (len_postings + 0.5) + 1)

    def _tfidf(self, docid, freq, len_postings):
        return self._tf(docid, freq) * self._idf(len_postings)

    def _bm25(self, docid, freq, len_postings):
        return self._idf(len_postings) * (
            (freq * (self._bm25_k1 + 1)) /
            freq + self._bm25_k1 * (1 - self._bm25_b + self._bm25_b *
                                    self._url_mapping.get_doc_len(docid) /
                                    self._avg_doc_len)
        )

    def _top10_json(self, query: str, scores: ScoreHeap):
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
                "URL": self._url_mapping.get_url(docid),
                "Score": round(score, 1),
            })

        result_json = {}
        result_json["Query"] = query
        result_json["Results"] = results

        return result_json
