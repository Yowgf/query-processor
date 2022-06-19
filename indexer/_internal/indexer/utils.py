import os

from common.log import log

logger = log.logger()

def write_index(index, outfpath, docid_offset):
    logger.info(f"Writing index to '{outfpath}'")
    with open(outfpath, 'a', encoding='utf-8') as outf:
        sorted_words = sorted(list(index.keys()))
        for word in sorted_words:
            outf.write(word)
            inverted_list = index[word]
            for entry in inverted_list:
                outf.write(f" {docid_offset + entry[0]},{entry[1]}")
            outf.write("\n")
    logger.info(f"Successfully wrote index to '{outfpath}'")

def move_file(infpath, outfpath, max_read_chars):
    logger.info(f"Moving index from '{infpath}' to '{outfpath}'")
    with open(infpath, "r") as inf:
        with open(outfpath, "a") as outf:
            while True:
                s = inf.read(max_read_chars)
                if len(s) == 0:
                    break
                outf.write(s)
    os.remove(infpath)
    logger.info(f"Successfully moved index from '{infpath}' to '{outfpath}'")

def is_useful_warcio_record(record):
    return (record.rec_type == 'response' and
            record.http_headers != None and
            record.http_headers.get_header('Content-Type') == 'text/html' or
            record.http_headers.get_header('Content-Type') == 'application/http')

def get_warcio_record_url(record):
    return record.rec_headers.get_header('WARC-Target-URI')
