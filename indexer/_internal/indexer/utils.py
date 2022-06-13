from common.log import log

logger = log.logger()

def read_index(infpath, checkpoint, max_read_chars):
    logger.info(f"Reading index from '{infpath}'.")

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
        s = stream.read()
        if s == '':
            checkpoint = None
        else:
            checkpoint = stream.tell()

    logger.info(f"Read {len(index_str)} chars from '{infpath}'.")

    logger.info(f"Processing inverted lists.")

    inverted_lists = index_str.split("\n")
    del(index_str)
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
    del(inverted_lists)

    logger.info(f"Successfully processed inverted lists.")

    return index, checkpoint

def write_index(index, outfpath, docid_offset):
    logger.info(f"Writing index to '{outfpath}'")
    with open(outfpath, 'a') as outf:
        for word in index:
            outf.write(word)
            inverted_list = index[word]
            for entry in inverted_list:
                outf.write(f" {docid_offset + entry[0]},{entry[1]}")
            outf.write("\n")
    logger.info(f"Successfully wrote index to '{outfpath}'")

def merge_indexes(index1, index2):
    logger.info("Merging indexes")

    merged_index = {}
    for word in index1:
        if word in index2:
            merged_index[word] = sorted(index1[word] + index2[word])
        else:
            merged_index[word] = index1[word]

    logger.info("Successfully merged indexes")

    return merged_index

def is_useful_warcio_record(record):
    return (record.rec_type == 'response' and
            record.http_headers != None and
            record.http_headers.get_header('Content-Type') == 'text/html' or
            record.http_headers.get_header('Content-Type') == 'application/http')

def get_warcio_record_url(record):
    return record.rec_headers.get_header('WARC-Target-URI')
