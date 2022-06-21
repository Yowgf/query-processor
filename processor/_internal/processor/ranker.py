import concurrent.futures
import gc
import json
from math import log as natural_log
import threading

from common.log import log
from common.memory.defs import MEGABYTE
from common.memory.utils import sizeof
from common.utils.index import (read_index,
                                index_docids)
from common.utils.index_metadata import read_index_metadata
from common.utils.url_mapping import read_url_mapping
from common.preprocessing.normalize import tokenize_and_normalize
from .utils import (preprocess_entire_index,
                    find_checkpoints_marks,
                    subindex_from_words_marks)
from .score_heap import ScoreHeap

logger = log.logger()

MAX_READ_CHARS    = 256 * MEGABYTE
NUM_RESULTS       = 10
RANKER_TYPE_TFIDF = "TFIDF"
RANKER_TYPE_BM25  = "BM25"

class Ranker:
    def __init__(self, ranker_type: str, index_fpath: str, parallelism: int = None,
                 benchmarking: bool = None):
        self._index_fpath = index_fpath
        self._checkpoint = 0
        self._max_num_thread = parallelism or 4
        self._benchmarking = benchmarking

        if ranker_type not in [RANKER_TYPE_TFIDF, RANKER_TYPE_BM25]:
            raise ValueError(f"Invalid ranker type {ranker_type}")
        self._ranker_type = ranker_type

        # k1 in [1.2, 2.0]
        self._bm25_k1 = 1.5
        self._bm25_b = 0.75

        self._max_num_thread = 4

    def init(self, queries):
        logger.info("Initializing ranker")

        url_mapping, checkpoint = read_url_mapping(self._index_fpath, 0)

        index_metadata, checkpoint = read_index_metadata(self._index_fpath,
                                                         checkpoint)

        self._tokens = {}
        all_tokens = []
        for query in queries:
            tokenized_query = tokenize_and_normalize(query)
            self._tokens[query] = tokenized_query
            for word in tokenized_query:
                if word not in all_tokens:
                    all_tokens.append(word)
        self._url_mapping = url_mapping
        self._num_docs = index_metadata.num_docs
        self._max_docid = index_metadata.max_docid
        self._avg_doc_len = index_metadata.avg_doc_len

        #logger.info(f"Size of subindex: {sizeof(self._subindex)}.")
        #logger.info(f"Size of url_mapping: {sizeof(self._url_mapping._m)}")

        # URL mapping takes a lot of memory. We want to filter it to contain
        # only documents relevant to the queries.
        #
        # preprocess_entire_index also returns marks every MB of the file, for
        # easy access by slave threads.
        subindex, marks, words_not_found = preprocess_entire_index(
            self._index_fpath, checkpoint, all_tokens)
        all_docids = index_docids(subindex)
        #self._url_mapping.filter_docids(all_docids)
        logger.info(f"Size of url_mapping: {sizeof(self._url_mapping._m)}")
        del subindex, all_docids
        self._marks = marks
        self._words_not_found = set(words_not_found)
        gc.collect()

        # Delete tokens that are not found in the index. This preprocessing can
        # reduce the number of loops traversed in the DAAT matching.
        for query in self._tokens:
            tokens = self._tokens[query]
            for i in range(len(tokens)-1, -1, -1):
                if tokens[i] in words_not_found:
                    logger.info(f"Token '{tokens[i]}' not found in index")
                    self._tokens[query].pop(i)

        logger.info("Successfully initialized ranker")

    # rank uses internally stored queries, initialized in the init() function.
    #
    # This function is run by the master thread, which initializes one thread
    # per query (of course with a maximum number of threads), and waits in a
    # join.
    def rank_all(self):
        logger.info(f"Ranking queries: {list(self._tokens.keys())}")

        gc.collect()

        results = []
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=self._max_num_thread
        ) as executor:
            futures = []
            for query in self._tokens:
                futures.append(executor.submit(self._rank, query,
                                               self._tokens[query]))
            completed, not_completed = concurrent.futures.wait(
                futures,
                return_when=concurrent.futures.ALL_COMPLETED,
            )
            assert len(not_completed) == 0
            for future in completed:
                results.append(future.result())

        logger.info(f"Successfully ranked queries: {list(self._tokens.keys())}")

        return results

    # _rank is executed by each slave thread.
    def _rank(self, query, tokens):
        tid = threading.get_ident()

        try:
            logger.info(f"({tid}) Ranking query: '{query}'. Tokens: "+
                        f"{self._tokens[query]}")

            tokens = self._tokens[query]
            checkpoints = find_checkpoints_marks(self._marks, tokens, tid)
            subindex = subindex_from_words_marks(self._index_fpath, checkpoints,
                                                 tokens, tid)

            scores = self._score(subindex, tokens)
            result = self._top10_json(query, scores)
            result_json = json.dumps(result, ensure_ascii=False)

        except Exception as e:
            logger.error(f"({tid}) Received unexpected exception: "+
                         f"{e}. Returning immediately.", exc_info=True)

        logger.info(f"({tid}) Successfully ranked query: '{query}'. "+
                    f"Result length: {len(result)}")

        return result_json

    # Scores documents in a Document at a time (DAAT) fashion.
    def _score(self, subindex, tokens):
        logger.info(f"Scoring tokens {tokens} with subindex of length: "+
                    f"{len(subindex)}")

        if len(tokens) == 0:
            logger.info(f"Did not find any of the tokens. Returning empty score.")
            return ScoreHeap()

        # DAAT adapted from 2022-01 Information Retrieval class slides
        #
        scores = ScoreHeap()
        if self._benchmarking:
            scores_list = []
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
                        # Scoring policy
                        if self._ranker_type == RANKER_TYPE_TFIDF:
                            score += self._tfidf(docid, weight, len(subindex[term]))
                        elif self._ranker_type == RANKER_TYPE_BM25:
                            score += self._bm25(docid, weight, len(subindex[term]))

                        posting_idx += 1
                        break
                    posting_idx += 1

                posting_idxs[term] = posting_idx

            if score != 0:
                scores.push(docid, score)
                if self._benchmarking:
                    scores_list.append(score)
        if self._benchmarking:
            print(json.dumps(scores_list))

        logger.info(f"Successfully scored {tokens} with subindex len "+
                    f"{len(subindex)}. Scores length: {len(scores)}")

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
            # while len(scores) > 0:
            #     new_docid, new_score = scores.pop()
            #     if new_docid != docid:
            #         # Put back
            #         scores.push(new_docid, new_score)
            #         break
            #     score += new_score

            results.append({
                "URL": self._url_mapping.get_url(docid),
                "Score": round(score, 1),
            })

        result_json = {}
        result_json["Query"] = query
        result_json["Results"] = results

        return result_json
