from common.log import log

logger = log.logger()

BEGIN_INDEX_METADATA = "-----BEGIN INDEX METADATA-----\n"
END_INDEX_METADATA   = "-----END INDEX METADATA-----\n"

NUM_DOCS_KEY    = "num_docs"
MAX_DOCID_KEY = "max_docid"
AVG_DOC_LEN_KEY = "avg_doc_len"

class IndexMetadata:
    def __init__(self, metadata):
        logger.info(f"Initializing index metadata: {metadata}")
        self.num_docs = int(metadata[NUM_DOCS_KEY])
        self.max_docid = int(metadata[MAX_DOCID_KEY])
        self.avg_doc_len = float(metadata[AVG_DOC_LEN_KEY])

def read_index_metadata(infpath, checkpoint):
    metadata = {}
    with open(infpath, "r") as f:
        f.seek(checkpoint)

        first_line = f.readline()
        assert first_line == BEGIN_INDEX_METADATA, first_line
        s = ""
        newline = f.readline()
        while newline != END_INDEX_METADATA:
            s += newline
            newline = f.readline()
        lines = s.rstrip().split("\n")
        for line in lines:
            name_values = line.split(" ")
            name = name_values[0]
            values = name_values[1:]
            if len(values) == 1:
                values = values[0]
            metadata[name] = values

        checkpoint = f.tell()

    return IndexMetadata(metadata), checkpoint
