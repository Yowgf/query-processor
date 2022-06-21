from common.log import log
from common.utils.utils import read_max

logger = log.logger()

# read_index reads the inverted index in given file. It tries to use as little
# memory as possible, based on the argument max_read_chars.
def read_index(infpath, checkpoint, max_read_chars):
    logger.info(f"Reading index from '{infpath}' with checkpoint {checkpoint}. "+
                f"Max chars allowed to read: {max_read_chars}.")

    index = {}

    index_str, checkpoint = read_max(infpath, checkpoint, max_read_chars)

    logger.info(f"Processing inverted lists.")

    inverted_lists = index_str.split("\n")
    del index_str
    for inverted_list in inverted_lists:
        word, postings = postings_from_str(inverted_list)
        if len(postings) > 1:
            index[word] = postings

    if len(inverted_lists) > 0:
        del inverted_lists

    logger.info(f"Successfully processed inverted lists.")

    return index, checkpoint

def postings_from_str(s):
    split_by_space = s.strip().split(" ")
    if len(split_by_space) <= 1:
        return '', []
    word = split_by_space[0]
    postings = []
    for docfreq_str in split_by_space[1:]:
        docfreq_split = docfreq_str.split(",")
        postings.append((int(docfreq_split[0]), int(docfreq_split[1])))
    del split_by_space
    return word, postings

def index_docids(index):
    all_docids = set()
    for word in index:
        postings = index[word]
        docids = [posting[0] for posting in postings]
        all_docids = all_docids.union(docids)
    return all_docids
