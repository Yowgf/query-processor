from contextlib import contextmanager
import os
from shutil import rmtree
import sys

from common.log import log

logger = log.logger()

@contextmanager
def suppress_output():
    with open(os.devnull, 'w') as devnull:
        old_stdout = sys.stdout
        sys.stdout = devnull
        try:
            yield
        finally:
            sys.stdout = old_stdout

def truncate_file(fpath):
    open(fpath, 'w')

def truncate_dir(dpath):
        try:
            os.stat(dpath)
            rmtree(dpath)
        except FileNotFoundError:
            pass
        os.mkdir(dpath)

def read_max(infpath, checkpoint, max_read_chars):
    with open(infpath, 'r', encoding='utf-8') as stream:
        stream.seek(checkpoint)
        index_str = stream.read(max_read_chars)
        if len(index_str) == 0:
            return '', None
        if index_str[-1] != '\n':
            index_str += stream.readline()
        assert index_str[-1] == '\n', "input subindex file is malformed"

        # Mark checkpoint as None if reached EOF
        s = stream.read(1)
        if s == '':
            checkpoint = None
        else:
            checkpoint = stream.tell() - len(s.encode('utf-8'))

    logger.info(f"Read {len(index_str)} chars from '{infpath}'.")

    return index_str, checkpoint
