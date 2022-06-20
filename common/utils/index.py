from common.log import log

logger = log.logger()

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

def index_docids(index):
    all_docids = set()
    for word in index:
        postings = index[word]
        docids = [posting[0] for posting in postings]
        all_docids = all_docids.union(docids)
    return all_docids
