from common.log import log
from common.utils.url_mapping import (BEGIN_URL_MAPPING, END_URL_MAPPING)

logger = log.logger()

def write_url_mapping_begin(outfpath):
    with open(outfpath, "a") as f:
        f.write(BEGIN_URL_MAPPING)

def write_url_mapping_end(outfpath):
    with open(outfpath, "a") as f:
        f.write(END_URL_MAPPING)

def write_url_mapping(url_mapping, outfpath):
    logger.info(f"Writing URL mapping of size {len(url_mapping)}")
    with open(outfpath, "a") as f:
        for docid in url_mapping:
            f.write(f"{docid} {url_mapping[docid]}\n")
    logger.info(f"Successfully wrote URL mapping of size {len(url_mapping)}")

def skip_url_mapping(infpath):
    logger.info(f"Skipping URL mapping from '{infpath}'")

    checkpoint = None
    with open(infpath, "r") as f:
        line = None
        while line != '' and line != END_URL_MAPPING:
            line = f.readline()
        checkpoint = f.tell()

    logger.info(f"Successfully skipped URL mapping from '{infpath}'. "+
                f"Checkpoint: {checkpoint}")

    return checkpoint
