from common.log import log

logger = log.logger()

BEGIN_INDEX_METADATA = "-----BEGIN INDEX METADATA-----\n"
END_INDEX_METADATA   = "-----END INDEX METADATA-----\n"

NUM_DOCS_KEY = "num_docs"

class IndexMetadata:
    def __init__(self, metadata):
        logger.info(f"Initializing index metadata: {metadata}")
        self.num_docs = int(metadata[NUM_DOCS_KEY])

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

def read_index(infpath, checkpoint, max_read_chars):
    logger.info(f"Reading index from '{infpath}' with checkpoint {checkpoint}. "+
                f"Max chars allowed to read: {max_read_chars}.")

    index = {}

    read_whole_file = False
    with open(infpath, 'r', encoding='utf-8') as stream:
        stream.seek(checkpoint)
        index_str = stream.read(max_read_chars)
        if len(index_str) == 0:
            return index, None
        extra_bytes = 0
        while index_str[-1] != '\n':
            new_char = stream.read(1)
            if len(new_char) == 0:
                checkpoint = None
                break
            index_str += new_char
            extra_bytes += len(new_char.encode('utf-8'))
        assert index_str[-1] == '\n' and checkpoint != None, (
            "input subindex file is malformed")

        # Mark checkpoint as None if reached EOF
        s = stream.read(1)
        if s == '':
            checkpoint = None
        else:
            checkpoint = stream.tell() - len(s.encode('utf-8'))

    logger.info(f"Read {len(index_str)} chars from '{infpath}'.")

    logger.info(f"Processing inverted lists.")

    inverted_lists = index_str.split("\n")
    del index_str
    for inverted_list in inverted_lists:
        split_by_space = inverted_list.strip().split(" ")
        if len(split_by_space) <= 1:
            continue

        word = split_by_space[0]
        index[word] = []
        for docfreq_str in split_by_space[1:]:
            docfreq_split = docfreq_str.split(",")
            docfreq = (int(docfreq_split[0]), int(docfreq_split[1]))
            index[word].append(docfreq)
    if len(inverted_lists) > 0:
        del inverted_lists
        del split_by_space

    logger.info(f"Successfully processed inverted lists.")

    return index, checkpoint
