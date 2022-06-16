import concurrent.futures
from datetime import datetime
import gc
import glob
import os
import shutil
from threading import get_ident
from typing import Mapping, List, Tuple

from warcio.archiveiterator import ArchiveIterator
import nltk

from .parser import PlaintextParser
from .statistics import Statistics
from .subindex import Subindex
from .utils import read_index
from .utils import write_index
from .utils import merge_indexes
from .utils import is_useful_warcio_record
from .utils import get_warcio_record_url
from common.log import log
from common.memory.defs import MEGABYTE
from common.memory.limit import memory_limit
from common.memory.tracker import log_memory_usage
from common.utils.utils import truncate_file
from common.utils.utils import truncate_dir

logger = log.logger()

# TODOs:
#
# - Include printed information according to specification.
#
# - Log the file associated with each subindex, as well as the limit 12_000, so
# - that it is possible to trace back the documents that contain some word with
# - the final index.
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
        # truncate_file(self._output_file)
        # truncate_dir(self._subindexes_dir)

    # _init_limits assumes that the corpus files have already been located.
    def _init_limits(self):
        safe_memory_margin = 0.5
        min_docs_per_process = 50

        # The maximum of processes was imposed due to specific requirements of
        # the assignment. This should be more flexible in a production-level
        # implementation.
        self._max_num_process = int(min(8, len(self._corpus_files)))
        safe_memory_limit = int(
            (self._memory_limit * safe_memory_margin) / self._max_num_process
        )
        self._max_docs_per_process = int(max(
            min_docs_per_process,
            safe_memory_limit / (self._estimate_max_memory_consumed_per_doc *
                                 self._max_num_process)
        ))
        self._num_subindexes = min(2 * self._max_num_process,
                                   len(self._corpus_files))

        self._memory_per_subprocess = int(
            self._memory_limit / (self._max_num_process + 1)
        )

        # No more than 12K docs per file
        self._max_docs_in_file = 12_000

        self._max_read_chars = int(
            max(1, (self._memory_limit / 1024) ** 2 * 8)) * MEGABYTE

        # Print limits in alphabetical order.
        logger.info(f"Limit max_docs_in_file={self._max_docs_in_file}")
        logger.info(f"Limit max_docs_per_process={self._max_docs_per_process}")
        logger.info(f"Limit max_num_process={self._max_num_process}")
        logger.info(f"Limit max_read_chars={self._max_read_chars}")
        logger.info(f"Limit memory_per_subprocess={self._memory_per_subprocess}")
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

        # Define the document offset of each subindex.
        num_files = 0
        for subindex in self._subindexes:
            subindex.docid_offset = num_files * self._max_docs_in_file
            num_files += len(subindex)

    def run(self):
        before = datetime.now()

        with concurrent.futures.ProcessPoolExecutor(
                max_workers=self._max_num_process
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

        try:
            del executor
            gc.collect()        
        except Exception as e:
            logger.info(f"Error cleaning up memory from indexes production: {e}",
                        exc_info=True)

        self._merge_index()

        elapsed_secs = (datetime.now() - before).seconds

        statistics = self._gather_statistics()
        statistics.set_elapsed_time(elapsed_secs)

        print(statistics.to_json())

        self._cleanup()

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
        try:
            os.rmdir(self._subindexes_dir)
        except FileNotFoundError:
            pass

    # _gather_statistics reads the final output file, counting the number of
    # lists etc to generate final statistics for the indexer run.
    def _gather_statistics(self):
        index_size = int(os.stat(self._output_file).st_size / MEGABYTE)

        num_lists = 0
        list_sizes = []
        avg_list_size = 0

        checkpoint = 0
        while checkpoint != None:
            index, checkpoint = read_index(
                self._output_file, checkpoint, self._max_read_chars)

            for word in index:
                list_sizes.append(len(index[word]))
                num_lists += 1

            del index
            gc.collect()

        avg_list_size = round(sum(list_sizes) / len(list_sizes), 1)

        statistics = Statistics()
        statistics.set_index_size(index_size)
        statistics.set_num_lists(num_lists)
        statistics.set_avg_list_size(avg_list_size)

        return statistics

    def _run(self, subindex):
        memory_limit(self._memory_per_subprocess)

        tid = get_ident()

        old_docid = subindex.docid
        try:
            fpath, old_checkpoint = subindex.pop_file()

            docs, completed, checkpoint = self._streamize(fpath, old_checkpoint, tid)
            tokenized_docs = self._tokenize(docs, tid)
            del docs
            preprocessed_docs = self._preprocess(tokenized_docs, tid)
            del tokenized_docs
            gc.collect()
            index, new_docid = self._produce_index(subindex, preprocessed_docs, tid)
            self._flush_index(subindex, index, tid)
            # Only increment subindex docid after really done with portion of
            # index.
            subindex.docid = new_docid
            del index
            gc.collect()
        except Exception as e:
            logger.error(f"({tid}) Received unexpected exception: "+
                         f"{e}. Returning immediately.", exc_info=True)
            try:
                # Here we don't care if the file was fully processed or not. We
                # need to restore the previous state so that we can try again.
                subindex.docid = old_docid
                subindex.push_file(fpath, old_checkpoint)
                return subindex, False
            except Exception as e:
                logger.error(f"({tid}) Error pushing file to subindex: {e}.")
                return subindex, False

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
                if len(new_docs) >= self._max_docs_per_process:
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
                if len(normalized_word) < 3:
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
        docid = subindex.docid
        for doc in preprocessed_docs:
            word_freq = preprocessed_docs[doc]
            for word in word_freq:
                freq = word_freq[word]
                if word not in index:
                    index[word] = []
                index[word].append((docid, freq))
            docid += 1

        logger.info(f"({tid}) Successfully indexed docs")
        logger.debug(f"({tid}) Index result: {index}")
        log_memory_usage(logger)

        return index, docid

    def _flush_index(self, subindex, index, tid="Unknown"):
        outfpath = (f"{self._subindexes_dir}/"+
                    f"{subindex.id}_{subindex.docid}_{self._output_file}")

        logger.info(f"({tid}) Flushing index to path '{outfpath}'")
        log_memory_usage(logger)

        write_index(index, outfpath, subindex.docid_offset)

        logger.info(f"({tid}) Successfully flushed index to path '{outfpath}'")
        log_memory_usage(logger)

    def _merge_index(self):
        logger.info(f"Merging index from dir '{self._subindexes_dir}' to file "+
                    f"'{self._output_file}'")
        log_memory_usage(logger)

        fpaths = glob.glob(f"{self._subindexes_dir}/*")
        if len(fpaths) == 0:
            return
        if len(fpaths) == 1:
            shutil.move(fpaths.pop(), self._output_file)
            return

        while len(fpaths) > 1:
            log_memory_usage(logger)

            index1_fpath = fpaths.pop(0)
            index2_fpath = fpaths.pop(0)
            merged_index_outfpath = index2_fpath + "_"

            checkpoint1 = 0
            checkpoint2 = 0
            while checkpoint1 != None:
                index1, checkpoint1 = read_index(index1_fpath, checkpoint1,
                                                 self._max_read_chars)
                logger.info(f"Read index 1. Size: {len(index1)}")

                if checkpoint2 != None:
                    sorted_words_index1 = sorted(list(index1.keys()))
                while checkpoint2 != None:
                    if len(sorted_words_index1) == 0:
                        break

                    index2, checkpoint2 = read_index(index2_fpath, checkpoint2,
                                                     self._max_read_chars)
                    logger.info(f"Read index 2. Size: {len(index2)}")
                    if len(index2) == 0:
                        continue

                    sorted_words_index2 = sorted(list(index2.keys()))
                    last_index1 = len(sorted_words_index1) - 1
                    while checkpoint1 != None:
                        while (sorted_words_index2[-1] <
                               sorted_words_index1[last_index1] and last_index1 > 0
                        ):
                            last_index1 -= 1
                        if last_index1 != len(sorted_words_index1) - 1:
                            break

                        for word in sorted_words_index1:
                            if word in index2:
                                index2[word] = sorted(index2[word] + index1[word])
                            else:
                                index2[word] = index1[word]
                        del index1
                        del sorted_words_index1
                        gc.collect()

                        # If index1 is entirely within index2, we should read
                        # new piece of index1 from file, to make sure that we
                        # aren't missing out on some piece of index1 when
                        # merging with index2.
                        #
                        # Example:
                        #
                        # index1 = {
                        #  "a": [(1,2)],
                        #  "b": [(1,1)]
                        # }
                        #
                        # index2 = {
                        #  "b": [(3,4)]
                        # }
                        #
                        # Notice that b >= b >= a, so last_index1 == 0
                        #
                        index1, checkpoint1 = read_index(
                            index1_fpath, checkpoint1, self._max_read_chars)
                        logger.info(f"Read index 1 inside internal loop. "+
                                    f"Size: {len(index1)}")
                        sorted_words_index1 = sorted(list(index1.keys()))
                        last_index1 = len(sorted_words_index1) - 1
                        while (sorted_words_index2[-1] <
                               sorted_words_index1[last_index1] and last_index1 > 0
                        ):
                            last_index1 -= 1

                    for word in sorted_words_index1[:last_index1]:
                        if word in index2:
                            index2[word] = sorted(index2[word] + index1[word])
                        else:
                            index2[word] = index1[word]

                    # Remove merged words from index1
                    for word in sorted_words_index1[:last_index1 + 1]:
                        index1.pop(word)
                    sorted_words_index1 = sorted_words_index1[last_index1 + 1:]

                    write_index(index2, merged_index_outfpath, 0)
                    del index2
                    gc.collect()

                write_index(index1, merged_index_outfpath, 0)
                del index1
                gc.collect()

            shutil.move(merged_index_outfpath, index2_fpath)

            logger.info(f"Done with file '{index1_fpath}'")
            os.remove(index1_fpath)
            fpaths.append(index2_fpath)

        log_memory_usage(logger)
        shutil.move(fpaths.pop(), self._output_file)

        logger.info(f"Successfully merged index from dir '{self._subindexes_dir}'"+
                    f" to file '{self._output_file}'")
        log_memory_usage(logger)
