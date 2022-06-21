import threading

from common.log import log
from common.memory.defs import MEGABYTE
from common.utils.index import (read_index,
                                postings_from_str)

logger = log.logger()

INDEX_FILE_MARK_SPACING = MEGABYTE
INDEX_FILE_MUTEX = threading.Lock()

def first_word(s):
    if len(s) == 0:
        return ''
    i = 0
    word = ''
    while s[i] != ' ':
        word += s[i]
        i += 1
    return word

# Returns subindex with given terms, and a map of marks in the index file to
# facilitate traversal, each spaced by at least INDEX_FILE_MARK_SPACING bytes.
def preprocess_entire_index(index_fpath, checkpoint, words):
    logger.info(f"Preprocessing index '{index_fpath}' with words {words}")

    subindex = {}
    words_not_found = []
    marks = {}
    words_set = set(words)
    checkpoint_before = checkpoint
    while True:
        index, checkpoint = read_index(index_fpath, checkpoint,
                                       INDEX_FILE_MARK_SPACING)
        if checkpoint == None:
            break

        first_word = sorted(list(index.keys()))[0]
        if first_word not in marks:
            marks[first_word] = checkpoint_before
            logger.debug(f"Added mark '{first_word}': "+
                         f"{checkpoint_before}")
            checkpoint_before = checkpoint

        for word in words_set:
            if word in index:
                subindex[word] = index[word]

    for word in words_set:
        if word not in subindex:
            words_not_found.append(word)

    if len(words_not_found) > 0:
        logger.warning(f"The following words were not found in the index: "+
                       f"{words_not_found}")

    logger.info(f"Successfully preprocessed subindex for words {words}. Subindex "+
                f"length: {len(subindex)}. Marks length: {len(marks)}")

    return subindex, marks, words_not_found

def find_checkpoints_marks(marks, words, tid="Unknown"):
    checkpoints = []
    mark_words = sorted(list(marks.keys()))
    mark_words_idx = 0
    sorted_words = sorted(list(words))
    for word in sorted_words:
        if mark_words_idx >= len(mark_words):
            break
        while word > mark_words[mark_words_idx]:
            mark_words_idx += 1
        if mark_words_idx > 0:
            mark_words_idx -= 1
        logger.debug(f"Last mark for word '{word}': {mark_words[mark_words_idx]}")
        checkpoints.append(marks[mark_words[mark_words_idx]])
    logger.debug(f"({tid}) Checkpoints from words {sorted_words}: {checkpoints}")
    return checkpoints

# This is made to be accessed by slave threads, so we must take care to preserve
# mutual exclusion when accessing index file.
def subindex_from_words_marks(index_fpath, checkpoints, words, tid="Unknown"):
    INDEX_FILE_MUTEX.acquire()
    logger.info(f"({tid}) Generating subindex from file '{index_fpath}' and "+
                f"checkpoints {checkpoints}")
    subindex = {}
    words_set = set(words)
    for checkpoint in checkpoints:
        not_found = True
        while not_found:
            index, checkpoint = read_index(index_fpath, checkpoint,
                                           INDEX_FILE_MARK_SPACING)
            for word in words_set:
                if word == "regist":
                    logger.info(f"Index for regist: {sorted(list(index.keys()))}")
                if word in index:
                    not_found = False
                    subindex[word] = index[word]
        if len(subindex) == len(words_set):
            break
    logger.info(f"({tid}) Successfully generated subindex from file "+
                f"'{index_fpath}' and checkpoints {checkpoints}. Subindex length: "+
                f"{len(subindex)}")
    INDEX_FILE_MUTEX.release()
    return subindex
