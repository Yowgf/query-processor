from common.log import log

logger = log.logger()

def find_word_postings(index_fpath, checkpoint, word):
    with open(index_fpath, "r") as f:
        f.seek(checkpoint)

        while True:
            newline = f.readline()
            if newline == '':
                return [], None

            entries = newline.rstrip().split(" ")
            if entries[0] == word:
                break

        postings = []
        for entry in entries[1:]:
            split_by_comma = entry.split(",")
            docid = int(split_by_comma[0])
            freq = int(split_by_comma[1])
            postings.append((docid, freq))

        logger.debug(f"Length of posting for word '{word}': {len(postings)}")

        checkpoint = f.tell()

    return postings, checkpoint

def subindex_with_words(index_fpath, checkpoint, words):
    logger.info(f"Generating subindex for words {words}")

    subindex = {}
    words_not_found = []

    words_set = set(words)
    parsed_whole_file = False
    with open(index_fpath, "r") as f:
        while True:
            while True:
                newline = f.readline()
                if newline == '':
                    parsed_whole_file = True
                    break

                entries = newline.rstrip().split(" ")
                word = entries[0]
                if word in words_set:
                    break
            if parsed_whole_file:
                break

            postings = []
            for entry in entries[1:]:
                split_by_comma = entry.split(",")
                docid = int(split_by_comma[0])
                freq = int(split_by_comma[1])
                postings.append((docid, freq))
            subindex[word] = postings

            logger.debug(f"Length of posting for word '{word}': {len(postings)}")

    for word in words_set:
        if word not in subindex:
            words_not_found.append(word)

    if len(words_not_found) > 0:
        logger.warning(f"The following words were not found in the index: "+
                       f"{words_not_found}")

    logger.info(f"Successfully generated subindex for words {words}. Subindex "+
                f"length: {len(subindex)}")

    return subindex
