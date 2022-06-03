import glob
import shutil
import os

from warcio.archiveiterator import ArchiveIterator
import nltk

from .parser import HtmlParser
from .utils import read_index
from .utils import write_index
from .utils import merge_indexes
from .utils import is_useful_warcio_record
from .utils import get_warcio_record_url
from common.log import log
from common.metrics.tracker import log_memory_usage
from common.utils.utils import suppress_output
from common.utils.utils import truncate_file
from common.utils.utils import truncate_dir

logger = log.logger()

# TODO: parallelize

class Indexer:
    _punctuations = set([',', '.', '[', ']', '(', ')', '{', '}', '»'])

    def __init__(self, config):
        self._corpus = config.corpus
        self._memory_limit = config.memory_limit
        self._output_file = config.output_file

        self._corpus_files = None
        self._index = {}
        self._docidx = 0
        self._subindexes_dir = "subindexes"

        # Dict filepath -> URL where we left off
        self._file_checkpoint = {}

        # TODO: adjust according to memory limit
        self._max_docs_per_file = 100

    def init(self):
        nltk.download('punkt', quiet=True)
        nltk.download('stopwords', quiet=True)
        self._stemmers = [nltk.stem.snowball.PortugueseStemmer(),
                          #nltk.stem.snowball.EnglishStemmer(),
        ]
        self._stopwords = set(nltk.corpus.stopwords.words('portuguese') +
                              nltk.corpus.stopwords.words('english'))

        self._corpus_files = glob.glob(self._corpus + "/*")

        truncate_file(self._output_file)
        truncate_dir(self._subindexes_dir)

    def run(self):
        doc_fpaths = self._corpus_files
        while len(doc_fpaths) > 0:
            for fpath in doc_fpaths:
                docs = self._streamize(fpath)
                tokenized_docs = self._tokenize(docs)
                preprocessed_docs = self._preprocess(tokenized_docs)
                self._produce_index(preprocessed_docs)
                self._flush_index()
            doc_fpaths = list(self._file_checkpoint.keys())
        self._merge_index()
        self._cleanup

    def _cleanup(self):
        os.rmdir(self._subindexes_dir)

    def _streamize(self, fpath):
        logger.info(f"Streamizing doc for path '{fpath}'")
        log_memory_usage(logger)

        new_docs = {}

        parsed_whole_file = True
        with open(fpath, 'rb') as stream:
            for record in ArchiveIterator(stream):
                # Get back to last checkpoint in the WARCIO file
                if fpath in self._file_checkpoint:
                    if is_useful_warcio_record(record):
                        url = get_warcio_record_url(record)
                        if url != self._file_checkpoint[fpath]:
                            continue

                if is_useful_warcio_record(record):
                    url = get_warcio_record_url(record)
                    page = record.content_stream().read()

                    # TODO: this parsing is taking a lot of time. Is there a way
                    # to avoid this?
                    parser = HtmlParser(page)
                    relevant_text = parser.find_text()
                    new_docs[url] = relevant_text
                    # logger.debug(f"For URL '{url}', added text: {relevant_text}")

                    if len(new_docs) >= self._max_docs_per_file:
                        self._file_checkpoint[fpath] = url
                        parsed_whole_file = False
                        break

        if parsed_whole_file:
            if fpath in self._file_checkpoint:
                self._file_checkpoint.pop(fpath)

        logger.info(f"Successfully streamized doc for path '{fpath}'")
        log_memory_usage(logger)

        return new_docs

    def _tokenize(self, docs):
        logger.info(f"Tokenizing docs")
        log_memory_usage(logger)

        tokenized_docs = docs
        for doc in docs:
            tokenized_docs[doc] = nltk.word_tokenize(docs[doc])

        logger.info(f"Successfully tokenized docs")
        logger.debug(f"Tokenized docs result: {tokenized_docs}")
        log_memory_usage(logger)

        return tokenized_docs

    def _preprocess(self, tokenized_docs):
        logger.info(f"Preprocessing docs")
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
                # Max 20 chars
                if len(normalized_word) > 20:
                    normalized_word = normalized_word[:20]

                # Increment frequency
                if normalized_word not in processed_word_freq:
                    processed_word_freq[normalized_word] = 0
                processed_word_freq[normalized_word] += 1

            preprocessed_docs[doc] = processed_word_freq

        logger.info(f"Successfully preprocessed docs")
        logger.debug(f"Preprocessed docs result: {preprocessed_docs}")
        log_memory_usage(logger)

        return preprocessed_docs

    def _produce_index(self, preprocessed_docs):
        logger.info(f"Indexing docs")
        log_memory_usage(logger)

        for doc in preprocessed_docs:
            word_freq = preprocessed_docs[doc]
            for word in word_freq:
                freq = word_freq[word]
                if word not in self._index:
                    self._index[word] = []
                self._index[word].append((self._docidx, freq))
            self._docidx += 1

        logger.info(f"Successfully indexed docs")
        logger.debug(f"Index result: {self._index}")
        log_memory_usage(logger)

    def _flush_index(self):
        outfpath = f"{self._subindexes_dir}/{self._docidx}_{self._output_file}"

        logger.info(f"Flushing index to path '{outfpath}'")
        log_memory_usage(logger)

        write_index(self._index, outfpath)
        self._index = {}

        logger.info(f"Successfully flushed index to path '{outfpath}'")
        log_memory_usage(logger)

    def _merge_index(self):
        logger.info(f"Merging index from dir '{self._subindexes_dir}' to file "+
                    f"'{self._output_file}'")
        log_memory_usage(logger)

        subindex_fpaths = glob.glob(f"{self._subindexes_dir}/*")
        if len(subindex_fpaths) == 0:
            return
        if len(subindex_fpaths) == 1:
            shutil.move(subindex_fpaths.pop(), self._output_file)
            return

        while len(subindex_fpaths) > 1:
            index1_fpath = subindex_fpaths.pop()
            index2_fpath = subindex_fpaths.pop()
            index1 = read_index(index1_fpath)
            index2 = read_index(index2_fpath)
            os.remove(index1_fpath)
            os.remove(index2_fpath)

            merged_index = merge_indexes(index1, index2)
            merged_index_outfpath = index1_fpath
            write_index(merged_index, merged_index_outfpath)

            subindex_fpaths = glob.glob(f"{self._subindexes_dir}/*")
        shutil.move(subindex_fpaths.pop(), self._output_file)

        logger.info(f"Successfully merged index from dir '{self._subindexes_dir}'"+
                    f" to file '{self._output_file}'")
        log_memory_usage(logger)
