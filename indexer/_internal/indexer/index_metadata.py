from common.log import log
from common.utils.index import (BEGIN_INDEX_METADATA, END_INDEX_METADATA)

logger = log.logger()

def write_index_metadata_begin(outfpath):
    with open(outfpath, "a") as f:
        f.write(BEGIN_INDEX_METADATA)

def write_index_metadata_end(outfpath):
    with open(outfpath, "a") as f:
        f.write(END_INDEX_METADATA)

def skip_index_metadata(infpath, checkpoint):
    logger.info(f"Skipping index metadata from '{infpath}'. Starting from "+
                f"checkpoint {checkpoint}")

    with open(infpath, "r") as f:
        f.seek(checkpoint)

        first_line = f.readline()
        assert first_line == BEGIN_INDEX_METADATA, first_line

        line = None
        while line != '' and line != END_INDEX_METADATA:
            line = f.readline()
        checkpoint = f.tell()

    logger.info(f"Successfully skipped index metadata from '{infpath}'. "+
                f"Checkpoint: {checkpoint}")

    return checkpoint
