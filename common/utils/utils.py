from contextlib import contextmanager
import os
from shutil import rmtree
import sys

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
