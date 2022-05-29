import glob

# Corpus streaming
from warcio.archiveiterator import ArchiveIterator

from .parser import HtmlParser

class Indexer:
    def __init__(self, config):
        self._corpus = config.corpus
        self._memory_limit = config.memory_limit
        self._output_file = config.output_file

        self._corpus_files = None
        self._index = []

    def init(self):
        self._corpus_files = glob.glob(self._corpus + "/*")

    def run(self):
        for fpath in self._corpus_files:
            self._streamize(fpath)
            break

    def _streamize(self, fpath):
        # TODO: find out file size before actually putting into memory
        with open(fpath, 'rb') as stream:
            for record in ArchiveIterator(stream):
                if record.rec_type == 'warcinfo':
                    print(record.raw_stream.read())
                    
                elif (record.rec_type == 'response' and
                      record.http_headers.get_header('Content-Type') == 'text/html'
                ):
                    url = record.rec_headers.get_header('WARC-Target-URI')
                    page = record.content_stream().read()
                    parser = HtmlParser(page)
                    relevant_text = parser.find_text()
                    if len(relevant_text) > 10:
                        print(f"Relevant text: {relevant_text}")
                        break

    def _tokenize(self, stream):
        pass

    def _stem(self, stream):
        pass

    def _index(self, stream):
        pass
