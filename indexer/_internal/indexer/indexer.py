import glob

# Corpus streaming
from warcio.archiveiterator import ArchiveIterator

from .parser import HtmlParser
from common.log import log
from common.metrics.tracker import log_memory_usage

logger = log.logger()

class Indexer:
    def __init__(self, config):
        self._corpus = config.corpus
        self._memory_limit = config.memory_limit
        self._output_file = config.output_file

        self._corpus_files = None
        self._index = []
        self._docidx = 0

    def init(self):
        self._corpus_files = glob.glob(self._corpus + "/*")

    def run(self):
        for fpath in self._corpus_files:
            docs = self._streamize(fpath)
            tokenized_docs = self._tokenize(docs)
            preprocessed_docs = self._preprocess(tokenized_docs)
            self._produce_index(preprocessed_docs)

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
                    logger.debug("For URL '{url}', added text: {relevant_text")

        logger.info(f"Successfully streamized doc for path '{fpath}'")
        log_memory_usage(logger)

        return new_docs

    def _tokenize(self, docs):
        logger.info(f"Tokenizing docs")
        log_memory_usage(logger)

        tokenized_docs = {}
        for doc in docs:
            pass

        logger.info(f"Successfully tokenized docs")
        log_memory_usage(logger)

        return tokenized_docs

    def _preprocess(self, docs):
        logger.info(f"Preprocessing docs")
        log_memory_usage(logger)

        preprocessed_docs = {}
        for doc in docs:
            pass

        logger.info(f"Successfully preprocessed docs")
        log_memory_usage(logger)

        return preprocessed_docs

    def _produce_index(self, docs):
        logger.info(f"Indexing docs")
        log_memory_usage(logger)

        # TODO: output index to self._output_file

        logger.info(f"Successfully indexed docs")
        log_memory_usage(logger)
