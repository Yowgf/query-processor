import glob
import os
from shutil import rmtree

from warcio.archiveiterator import ArchiveIterator
import nltk

from .parser import HtmlParser
from common.log import log
from common.metrics.tracker import log_memory_usage
from common.utils.utils import suppress_output

logger = log.logger()

# TODO: parallelize

class Indexer:
    def __init__(self, config):
        self._corpus = config.corpus
        self._memory_limit = config.memory_limit
        self._output_file = config.output_file

        self._corpus_files = None
        self._index = {}
        self._docidx = 0
        self._subindexes_dir = "subindexes"

    def init(self):
        nltk.download('punkt', quiet=True)
        nltk.download('stopwords', quiet=True)
        self._stemmer = nltk.stem.snowball.PortugueseStemmer()
        self._stopwords = set(nltk.corpus.stopwords.words('portuguese'))

        self._corpus_files = glob.glob(self._corpus + "/*")

        # Create if not exists
        try:
            os.stat(self._subindexes_dir)
            rmtree(self._subindexes_dir)
        except FileNotFoundError:
            pass
        os.mkdir(self._subindexes_dir)

    def run(self):
        for fpath in self._corpus_files:
            docs = self._streamize(fpath)
            tokenized_docs = self._tokenize(docs)
            preprocessed_docs = self._preprocess(tokenized_docs)
            self._produce_index(preprocessed_docs)
            self._flush_index()

    def _streamize(self, fpath):
        logger.info(f"Streamizing doc for path '{fpath}'")
        log_memory_usage(logger)

        new_docs = {}

        # TODO: find out file size before actually putting into memory
        with open(fpath, 'rb') as stream:
            for record in ArchiveIterator(stream):
                if (record.rec_type == 'response' and
                      record.http_headers.get_header('Content-Type') == 'text/html'
                ):
                    url = record.rec_headers.get_header('WARC-Target-URI')
                    page = record.content_stream().read()
                    parser = HtmlParser(page)
                    relevant_text = parser.find_text()
                    new_docs[url] = relevant_text

                    # logger.debug(f"For URL '{url}', added text: {relevant_text}")

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

                # Normalization
                normalized_word = self._stemmer.stem(word)

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

        with open(outfpath, 'a') as outf:
            for word in self._index:
                outf.write(word)
                inverted_list = self._index[word]
                for entry in inverted_list:
                    outf.write(f" {entry[0]},{entry[1]}")
                outf.write("\n")
        self._index = {}

        logger.info(f"Successfully flushed index to path '{outfpath}'")
        log_memory_usage(logger)
