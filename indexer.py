import sys
import resource
import argparse

from indexer.main import main as indexer_main

MEGABYTE = 1024 * 1024
def memory_limit(value):
    limit = value * MEGABYTE
    resource.setrlimit(resource.RLIMIT_AS, (limit, limit))

def main():
    indexer_main()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument(
        '-m',
        dest='memory_limit',
        action='store',
        required=True,
        type=int,
        help='memory available'
    )
    parser.add_argument(
        '-c',
        dest='corpus',
        action='store',
        required=True,
        type=str,
        help='path to a directory containing corpus WARC files'
    )
    parser.add_argument(
        '-i',
        dest='output_file',
        action='store',
        required=True,
        type=str,
        help='path to the index file to be generated'
    )
    args = parser.parse_args()
    memory_limit(args.memory_limit)
    try:
        main()
    except MemoryError:
        sys.stderr.write('\n\nERROR: Memory Exception\n')
        sys.exit(1)
