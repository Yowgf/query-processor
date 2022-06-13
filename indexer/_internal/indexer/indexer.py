import concurrent.futures
import gc
import glob
import os
import shutil
from threading import get_ident
from typing import Mapping, List, Tuple

from warcio.archiveiterator import ArchiveIterator
import nltk

from .parser import PlaintextParser
from .subindex import Subindex
from .utils import read_index
from .utils import write_index
from .utils import merge_indexes
from .utils import is_useful_warcio_record
from .utils import get_warcio_record_url
from common.log import log
from common.metrics.tracker import log_memory_usage
from common.utils.utils import truncate_file
from common.utils.utils import truncate_dir

logger = log.logger()

# TODOs:
#
# - make sure subindexes are being generated with right memory utilization by
#   letting the program run for a while.
#
# - test merging algorithm by letting it run for a while, and then over
#   night. If it is successful, we should have our final index at hands!
#
# - if possible and necessary, parallelize with pipeline
#
################################################################################

class Indexer:
    _punctuations = set([',', '.', '[', ']', '(', ')', '{', '}', '/', '\\']) # '»',

    _estimate_max_memory_consumed_per_doc = 0.4 # MB

    def __init__(self, config):
        self._corpus = config.corpus
        self._memory_limit = config.memory_limit
        self._output_file = config.output_file

        self._corpus_files = None
        self._index: Mapping[str, List[Tuple[int, int]]] = {}
        self._subindexes_dir = "subindexes"

        # Dict filepath -> URL where we left off
        self._file_checkpoint = {}

    # init is separated from __init__ because it might throw exceptions.
    def init(self):
        # The order in which the sub-init functions are called is very
        # important.
        logger.info("Initializing indexer.")
        self._init_nltk()
        self._init_files()
        self._init_limits()
        self._init_subindexes()
        logger.info("Successfully initialized indexer.")

    def _init_nltk(self):
        nltk.download('punkt', quiet=True)
        nltk.download('stopwords', quiet=True)
        self._stemmers = [nltk.stem.snowball.PortugueseStemmer(),
                          #nltk.stem.snowball.EnglishStemmer(),
        ]
        self._stopwords = set(nltk.corpus.stopwords.words('portuguese') +
                              nltk.corpus.stopwords.words('english'))

    def _init_files(self):
        self._corpus_files = glob.glob(self._corpus + "/*")
        truncate_file(self._output_file)
        truncate_dir(self._subindexes_dir)

    # _init_limits assumes that the corpus files have already been located.
    def _init_limits(self):
        safe_memory_margin = 0.5
        safe_memory_limit = self._memory_limit * safe_memory_margin
        min_docs_per_thread = 128

        self._absolute_limit_num_threads = int(min(50, len(self._corpus_files)))
        self._max_docs_per_thread = int(max(
            min_docs_per_thread,
            safe_memory_limit / (self._estimate_max_memory_consumed_per_doc *
                                 self._absolute_limit_num_threads)
        ))
        self._max_num_threads = int(
            safe_memory_limit / (self._estimate_max_memory_consumed_per_doc *
                                 self._max_docs_per_thread)
        )
        self._num_subindexes = min(2 * self._max_num_threads,
                                   len(self._corpus_files))
        # TODO: find out dinamically
        self._max_docs_per_subindex = 12_000

        logger.info(f"Limit absolute_limit_num_threads="+
                    f"{self._absolute_limit_num_threads}")
        logger.info(f"Limit max_num_threads={self._max_num_threads}")
        logger.info(f"Limit max_docs_per_thread={self._max_docs_per_thread}")
        logger.info(f"Limit num_subindexes={self._num_subindexes}")

    # _init_subindexes assumes that the corpus files have already been located.
    def _init_subindexes(self):
        self._subindexes = [Subindex(id) for id in range(self._num_subindexes)]
        subindex_id = 0
        file_idx = 0
        fpaths = self._corpus_files
        num_files = len(fpaths)
        while file_idx < num_files:
            for subindex_idx in range(self._num_subindexes):
                self._subindexes[subindex_idx].push_file(fpaths[file_idx])
                file_idx += 1
                if file_idx >= num_files:
                    break
        # TODO: fix this 'gambiarra'
        self._subindexes = [subindex for subindex in self._subindexes if len(subindex) > 0]

    def run(self):
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=self._max_num_threads
        ) as executor:
            results = []

            subindexes = self._subindexes

            while True:
                self._submit_jobs(executor, results, subindexes)
                subindexes = []

                completed, not_completed = concurrent.futures.wait(
                    results,
                    timeout=5,
                    return_when=concurrent.futures.FIRST_COMPLETED,
                )
                results = list(not_completed)
                if len(results) == 0:
                    logger.info("Stopping indexer: no jobs left.")
                    break

                for future in completed:
                    subindex = self._process_complete_job(future)
                    if subindex != None:
                        subindexes.append(subindex)

        self._merge_index()
        self._cleanup

    def _submit_jobs(self, executor, results, subindexes):
        for subindex in subindexes:
            results.append(executor.submit(self._run, subindex))

    def _process_complete_job(self, future):
        subindex, completed_subindex = future.result()

        if not completed_subindex:
            return subindex
        else:
            return None

    def _cleanup(self):
        os.rmdir(self._subindexes_dir)

    def _run(self, subindex):
        tid = get_ident()

        try:
            fpath, checkpoint = subindex.pop_file()

            docs, completed, checkpoint = self._streamize(fpath, checkpoint, tid)
            tokenized_docs = self._tokenize(docs, tid)
            preprocessed_docs = self._preprocess(tokenized_docs, tid)
            index = self._produce_index(subindex, preprocessed_docs, tid)
            self._flush_index(subindex, index, tid)

        except Exception as e:
            logger.error(f"({tid}) Received unexpected exception: "+
                         f"{e}. Returning immediately.", exc_info=True)

        try:
            if not completed:
                subindex.push_file(fpath, checkpoint)
        except Exception as e:
            logger.error(f"({tid}) Error pushing file to subindex: {e}.")

        completed_subindex = False
        if len(subindex) == 0:
            logger.info(f"({tid}) Completed subindex with id {subindex.id}")
            completed_subindex = True

        return subindex, completed_subindex

    def _streamize(self, fpath: str, checkpoint: int, tid="Unknown"):
        logger.info(f"({tid}) Streamizing doc for path '{fpath}', "+
                    f"with checkpoint {checkpoint}")
        log_memory_usage(logger)

        new_docs = {}
        parsed_whole_file = True
        with open(fpath, 'rb') as stream:

            for record in ArchiveIterator(stream):
                if stream.tell() < checkpoint:
                    continue

                url = get_warcio_record_url(record)
                text = record.content_stream().read()
                normalized_text = PlaintextParser.normalize_text(text)
                new_docs[url] = normalized_text
                # logger.debug(f"Added URL '{url}'")
                # logger.debug(f"For URL '{url}', added text: {normalized_text}")

                # TODO: We might need to add a stream EOF condition here
                if len(new_docs) >= self._max_docs_per_thread:
                    parsed_whole_file = False
                    checkpoint = stream.tell()
                    break

        logger.info(f"({tid}) Successfully streamized doc for path '{fpath}'")
        log_memory_usage(logger)

        return new_docs, parsed_whole_file, checkpoint

    def _tokenize(self, docs, tid="Unknown") -> Mapping[str, List[str]]:
        logger.info(f"({tid}) Tokenizing docs")
        log_memory_usage(logger)

        tokenized_docs = docs
        for doc in docs:
            tokenized_docs[doc] = nltk.word_tokenize(docs[doc])

        logger.info(f"({tid}) Successfully tokenized docs")
        logger.debug(f"({tid}) Tokenized docs result: {tokenized_docs}")
        log_memory_usage(logger)

        return tokenized_docs

    def _preprocess(self, tokenized_docs, tid="Unknown") -> Mapping[str, Mapping[str, int]]:
        logger.info(f"({tid}) Preprocessing docs")
        log_memory_usage(logger)

        preprocessed_docs = tokenized_docs
        for doc in tokenized_docs:
            doc_words = tokenized_docs[doc]

            # map word -> freq
            processed_word_freq = {}
            for word in doc_words:
                # Stopword removal
                if word in self._stopwords:
                    continue

                # Malformed words removal.
                # Examples: '', ',123', '.hello', '(me'.
                if word == '' or word[0] in self._punctuations:
                    continue

                # Normalization
                normalized_word = word
                for stemmer in self._stemmers:
                    normalized_word = stemmer.stem(normalized_word)
                # Min 3 chars
                if len(normalized_word) <= 2:
                    continue
                # Max 20 chars
                if len(normalized_word) > 20:
                    normalized_word = normalized_word[:20]

                # Increment frequency
                if normalized_word not in processed_word_freq:
                    processed_word_freq[normalized_word] = 0
                processed_word_freq[normalized_word] += 1

            preprocessed_docs[doc] = processed_word_freq

        logger.info(f"({tid}) Successfully preprocessed docs")
        logger.debug(f"({tid}) Preprocessed docs result: {preprocessed_docs}")
        log_memory_usage(logger)

        return preprocessed_docs

    def _produce_index(self, subindex, preprocessed_docs, tid="Unknown"):
        logger.info(f"({tid}) Indexing docs")
        log_memory_usage(logger)

        index = {}
        for doc in preprocessed_docs:
            word_freq = preprocessed_docs[doc]
            for word in word_freq:
                freq = word_freq[word]
                if word not in index:
                    index[word] = []
                index[word].append((subindex.docid, freq))
            subindex.docid += 1

        logger.info(f"({tid}) Successfully indexed docs")
        logger.debug(f"({tid}) Index result: {index}")
        log_memory_usage(logger)

        return index

    def _flush_index(self, subindex, index, tid="Unknown"):
        outfpath = (f"{self._subindexes_dir}/"+
                    f"{subindex.id}_{subindex.docid}_{self._output_file}")

        logger.info(f"({tid}) Flushing index to path '{outfpath}'")
        log_memory_usage(logger)

        docid_offset = subindex.id * self._max_docs_per_subindex
        write_index(index, outfpath, docid_offset)

        logger.info(f"({tid}) Successfully flushed index to path '{outfpath}'")
        log_memory_usage(logger)

    def _merge_index(self):
        logger.info(f"Merging index from dir '{self._subindexes_dir}' to file "+
                    f"'{self._output_file}'")
        log_memory_usage(logger)

        gc.collect()

        fpaths = glob.glob(f"{self._subindexes_dir}/*")
        if len(fpaths) == 0:
            return
        if len(fpaths) == 1:
            shutil.move(fpaths.pop(), self._output_file)
            return

        MB = 1024 * 1024
        max_read_chars = int(self._memory_limit / 64) * MB

        while len(fpaths) > 1:
            log_memory_usage(logger)

            index1_fpath = fpaths.pop(0)
            index2_fpath = fpaths.pop(0)
            
            checkpoint1 = 0
            while checkpoint1 != None:
                index1, checkpoint1 = read_index(index1_fpath, checkpoint1,
                                                 max_read_chars)
                logger.info(f"Read index 1. Size: {len(index1)}")
                
                checkpoint2 = 0
                while checkpoint2 != None:
                    index2, checkpoint2 = read_index(index2_fpath, checkpoint2,
                                                     max_read_chars)
                    logger.info(f"Read index 2. Size: {len(index2)}")
                    
                    merged_index = merge_indexes(index1, index2)
                    del(index2)
                    merged_index_outfpath = index2_fpath + "_"
                    write_index(merged_index, merged_index_outfpath, 0)
                    del(merged_index)
            del(index1)

            logger.info(f"Done with files '{index1_fpath}' and '{index2_fpath}'")
            os.remove(index1_fpath)
            os.remove(index2_fpath)
            shutil.move(merged_index_outfpath, index2_fpath)
            fpaths.append(index2_fpath)

        log_memory_usage(logger)
        shutil.move(fpaths.pop(), self._output_file)

        logger.info(f"Successfully merged index from dir '{self._subindexes_dir}'"+
                    f" to file '{self._output_file}'")
        log_memory_usage(logger)
