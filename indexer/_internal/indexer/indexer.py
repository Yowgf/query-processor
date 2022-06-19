import concurrent.futures
from datetime import datetime
import gc
import glob
import os
import shutil
from threading import get_ident
from typing import Mapping, List, Tuple

from warcio.archiveiterator import ArchiveIterator

from .parser import PlaintextParser
from .statistics import Statistics
from .subindex import Subindex
from .utils import (write_index,
                    move_file,
                    is_useful_warcio_record,
                    get_warcio_record_url)
from .index_metadata import (write_index_metadata_begin,
                             write_index_metadata_end,
                             skip_index_metadata)
from .url_mapping import (write_url_mapping_begin,
                          write_url_mapping_end,
                          write_url_mapping,
                          skip_url_mapping)
from common.utils.index import (NUM_DOCS_KEY,
                                MAX_DOCID_KEY,
                                AVG_DOC_LEN_KEY)
from common.log import log
from common.memory.defs import (MEGABYTE,
                                MAX_DOCS_PER_FILE)
from common.memory.limit import memory_limit
from common.memory.tracker import log_memory_usage
from common.utils.utils import (truncate_file,
                                truncate_dir)
from common.utils.index import read_index
from common.preprocessing.normalize import (tokenize,
                                            normalize_word)

logger = log.logger()

class Indexer:
    _estimate_max_memory_consumed_per_doc = 0.4 # MB

    def __init__(self, config):
        self._corpus = config.corpus
        self._memory_limit = config.memory_limit
        self._output_file = config.output_file

        self._corpus_files = None
        self._index: Mapping[str, List[Tuple[int, int]]] = {}
        self._subindexes_dir = "subindexes"
        self._urlmapping_dir = "urlmapping"

        # Dict filepath -> URL where we left off
        self._file_checkpoint = {}

        self._num_docs = 0
        self._max_docid = 0
        self._sum_doc_lens = 0

    # init is separated from __init__ because it might throw exceptions.
    def init(self):
        # The order in which the sub-init functions are called is very
        # important.
        logger.info("Initializing indexer.")
        self._init_files()
        self._init_limits()
        self._init_subindexes()
        logger.info("Successfully initialized indexer.")

    def _init_files(self):
        self._corpus_files = glob.glob(self._corpus + "/*")
        truncate_file(self._output_file)
        truncate_dir(self._subindexes_dir)
        truncate_dir(self._urlmapping_dir)

    # _init_limits assumes that the corpus files have already been located.
    def _init_limits(self):
        safe_memory_margin = 0.5
        min_docs_per_process = 25

        # The maximum of processes was imposed due to specific requirements of
        # the assignment. This should be more flexible in a production-level
        # implementation.
        self._max_num_process = int(min(6, len(self._corpus_files)))
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

        self._max_read_chars_subindex = int(
            (self._memory_limit / 1024) ** 2 * 8 * MEGABYTE
        )

        self._bytes_per_warcio_record = 16384
        self._max_read_bytes = int(
            (self._memory_limit / 1024) ** 2 * (
                self._bytes_per_warcio_record * 8
            )
        )

        # Print limits in alphabetical order.
        logger.info(f"Limit max_docs_per_process={self._max_docs_per_process}")
        logger.info(f"Limit max_num_process={self._max_num_process}")
        logger.info(f"Limit max_read_chars_subindex={self._max_read_chars_subindex}")
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
            subindex.docid_offset = num_files * MAX_DOCS_PER_FILE
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

                for future in completed:
                    subindex = self._process_complete_job(future)
                    if subindex != None:
                        subindexes.append(subindex)

                results = list(not_completed)
                if len(results) == 0:
                    logger.info("Stopping indexer: no jobs left.")
                    break
        try:
            del executor
            gc.collect()        
        except Exception as e:
            logger.info(f"Error cleaning up memory from indexes production: {e}",
                        exc_info=True)

        self._merge_url_mappings()
        self._append_index_metadata()
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
        subindex, completed_subindex, sum_doc_lens = future.result()

        self._sum_doc_lens += sum_doc_lens

        if not completed_subindex:
            return subindex
        else:
            self._num_docs += subindex.docid
            if subindex.docid_offset + subindex.docid > self._max_docid:
                self._max_docid = subindex.docid_offset + subindex.docid
            return None

    def _cleanup(self):
        try:
            shutil.rmtree(self._subindexes_dir)
            shutil.rmtree(self._urlmapping_dir)
        except FileNotFoundError:
            pass

    # _gather_statistics reads the final output file, counting the number of
    # lists etc to generate final statistics for the indexer run.
    def _gather_statistics(self):
        index_size = int(os.stat(self._output_file).st_size / MEGABYTE)

        num_lists = 0
        list_sizes = []
        avg_list_size = 0

        checkpoint = skip_url_mapping(self._output_file, 0)
        checkpoint = skip_index_metadata(self._output_file, checkpoint)
        while checkpoint != None:
            index, checkpoint = read_index(
                self._output_file, checkpoint, self._max_read_chars_subindex)

            for word in index:
                list_sizes.append(len(index[word]))
                num_lists += 1

            del index
            gc.collect()

        if len(list_sizes) == 0:
            avg_list_size = 0
        else:
            avg_list_size = round(sum(list_sizes) / len(list_sizes), 1)

        statistics = Statistics()
        statistics.set_index_size(index_size)
        statistics.set_num_lists(num_lists)
        statistics.set_avg_list_size(avg_list_size)

        return statistics

    def _run(self, subindex):
        memory_limit(self._memory_per_subprocess)

        gc.collect()

        pid = os.getpid()

        old_docid = subindex.docid
        sum_doc_lens = 0
        try:
            fpath, old_checkpoint = subindex.pop_file()

            docs, doc_lens, completed, checkpoint = self._streamize(
                fpath, old_checkpoint, pid)
            for length in doc_lens.values():
                sum_doc_lens += length
            tokenized_docs = self._tokenize(docs, pid)
            del docs
            preprocessed_docs = self._preprocess(tokenized_docs, pid)
            del tokenized_docs
            gc.collect()
            index, new_docid = self._produce_index(subindex, preprocessed_docs,
                                                   doc_lens, pid)
            self._flush_index(subindex, index, pid)
            # Only increment subindex docid after really done with portion of
            # index.
            subindex.docid = new_docid
            del index
            gc.collect()

        except Exception as e:
            logger.error(f"({pid}) Received unexpected exception: "+
                         f"{e}. Returning immediately.", exc_info=True)
            try:
                # Here we don't care if the file was fully processed or not. We
                # need to restore the previous state so that we can try again.
                subindex.docid = old_docid
                subindex.push_file(fpath, old_checkpoint)
                return subindex, False, 0
            except Exception as e:
                logger.error(f"({pid}) Error pushing file to subindex: {e}.")
                return subindex, False, 0

        try:
            if not completed:
                subindex.push_file(fpath, checkpoint)
        except Exception as e:
            logger.error(f"({pid}) Error pushing file to subindex: {e}.")

        completed_subindex = False
        if len(subindex) == 0:
            logger.info(f"({pid}) Completed subindex with id {subindex.id}")
            completed_subindex = True

        return subindex, completed_subindex, sum_doc_lens

    def _streamize(self, fpath: str, old_checkpoint: int, pid="Unknown"):
        logger.info(f"({pid}) Streamizing doc for path '{fpath}', "+
                    f"with old_checkpoint {old_checkpoint}")
        log_memory_usage(logger)

        new_docs = {}
        doc_lens = {}
        parsed_whole_file = True
        with open(fpath, 'rb') as stream:

            for record in ArchiveIterator(stream):
                if stream.tell() < old_checkpoint:
                    continue
                if (stream.tell() - old_checkpoint >=
                    self._max_read_bytes
                ):
                    parsed_whole_file = False
                    break

                url = get_warcio_record_url(record)
                text = record.content_stream().read()
                normalized_text = PlaintextParser.normalize_text(text)
                new_docs[url] = normalized_text

                doc_lens[url] = len(normalized_text)

            new_checkpoint = stream.tell()

        logger.info(f"({pid}) Successfully streamized doc for path '{fpath}'. "+
                    f"Size of docs: {len(new_docs)}. Number of bytes read: "+
                    f"{new_checkpoint - old_checkpoint}")
        log_memory_usage(logger)

        return new_docs, doc_lens, parsed_whole_file, new_checkpoint

    def _tokenize(self, docs, pid="Unknown") -> Mapping[str, List[str]]:
        logger.info(f"({pid}) Tokenizing docs")
        log_memory_usage(logger)

        for doc in docs:
            docs[doc] = tokenize(docs[doc])

        logger.info(f"({pid}) Successfully tokenized docs")
        logger.debug(f"({pid}) Tokenized docs len: {len(docs)}")
        log_memory_usage(logger)

        return docs

    def _preprocess(self, tokenized_docs, pid="Unknown") -> Mapping[str, Mapping[str, int]]:
        logger.info(f"({pid}) Preprocessing docs")
        log_memory_usage(logger)

        preprocessed_docs = tokenized_docs
        for doc in tokenized_docs:
            doc_words = tokenized_docs[doc]

            # map word -> freq
            processed_word_freq = {}
            for word in doc_words:
                normalized_word = normalize_word(word)
                if normalized_word == None:
                    continue

                # Increment frequency
                if normalized_word not in processed_word_freq:
                    processed_word_freq[normalized_word] = 0
                processed_word_freq[normalized_word] += 1

            preprocessed_docs[doc] = processed_word_freq

        logger.info(f"({pid}) Successfully preprocessed docs")
        logger.debug(f"({pid}) Preprocessed docs len: {len(preprocessed_docs)}")
        log_memory_usage(logger)

        return preprocessed_docs

    def _produce_index(self, subindex, preprocessed_docs, doc_lens, pid="Unknown"):
        logger.info(f"({pid}) Indexing docs")
        log_memory_usage(logger)

        urlmapping_fpath = (f"{self._urlmapping_dir}/"+
                            f"{subindex.id}_{subindex.docid}_"+
                            f"{self._output_file}")
        index = {}
        url_mapping = {}
        docid = subindex.docid
        for url in preprocessed_docs:
            url_mapping[docid + subindex.docid_offset] = (doc_lens[url], url)

            word_freq = preprocessed_docs[url]
            for word in word_freq:
                freq = word_freq[word]
                if word not in index:
                    index[word] = []
                index[word].append((docid, freq))
            docid += 1

        write_url_mapping(url_mapping, urlmapping_fpath)

        logger.info(f"({pid}) Successfully indexed docs")
        logger.debug(f"({pid}) Index result: {index}")
        log_memory_usage(logger)

        return index, docid

    def _flush_index(self, subindex, index, pid="Unknown"):
        outfpath = (f"{self._subindexes_dir}/"+
                    f"{subindex.id}_{subindex.docid}_{self._output_file}")

        logger.info(f"({pid}) Flushing index to path '{outfpath}'")
        log_memory_usage(logger)

        write_index(index, outfpath, subindex.docid_offset)

        logger.info(f"({pid}) Successfully flushed index to path '{outfpath}'")
        log_memory_usage(logger)

    def _merge_url_mappings(self):
        logger.info(f"Merging URL mapping from dir '{self._urlmapping_dir}' to "+
                    f"file '{self._output_file}'")
        log_memory_usage(logger)

        write_url_mapping_begin(self._output_file)

        fpaths = glob.glob(f"{self._urlmapping_dir}/*")
        if len(fpaths) == 0:
            write_url_mapping_end(self._output_file)
            return

        for infpath in fpaths:
            move_file(infpath, self._output_file, self._max_read_chars_subindex * 4)

        write_url_mapping_end(self._output_file)

        logger.info(f"Successfully merged URL mapping from dir "+
                    f"'{self._urlmapping_dir}' to file '{self._output_file}'")
        log_memory_usage(logger)

    def _append_index_metadata(self):
        logger.info(f"Appending index metadata to '{self._output_file}'")

        write_index_metadata_begin(self._output_file)

        num_docs = self._num_docs
        max_docid = self._max_docid
        avg_doc_len = self._sum_doc_lens / num_docs

        with open(self._output_file, "a") as f:
            f.write(f"{NUM_DOCS_KEY} {num_docs}\n")
            f.write(f"{MAX_DOCID_KEY} {max_docid}\n")
            f.write(f"{AVG_DOC_LEN_KEY} {avg_doc_len}\n")

        write_index_metadata_end(self._output_file)

        logger.info(f"Successfully appended index metadata to '{self._output_file}'")

    def _merge_index(self):
        logger.info(f"Merging index from dir '{self._subindexes_dir}' to file "+
                    f"'{self._output_file}'")
        log_memory_usage(logger)

        fpaths = glob.glob(f"{self._subindexes_dir}/*")
        if len(fpaths) == 0:
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
                                                 self._max_read_chars_subindex)
                logger.info(f"Read index 1. Size: {len(index1)}")

                if checkpoint2 != None:
                    sorted_words_index1 = sorted(list(index1.keys()))
                while checkpoint2 != None:
                    if len(sorted_words_index1) == 0:
                        break

                    index2, checkpoint2 = read_index(index2_fpath, checkpoint2,
                                                     self._max_read_chars_subindex)
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
                            index1_fpath, checkpoint1, self._max_read_chars_subindex)
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
        move_file(fpaths.pop(), self._output_file, self._max_read_chars_subindex*4)

        logger.info(f"Successfully merged index from dir '{self._subindexes_dir}'"+
                    f" to file '{self._output_file}'")
        log_memory_usage(logger)
