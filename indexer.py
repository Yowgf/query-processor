import sys
import resource
import argparse

from common.log import log
from indexer.main import main as indexer_main

logger = log.logger()

MEGABYTE = 1024 * 1024
def memory_limit(value):
    limit = value * MEGABYTE
    resource.setrlimit(resource.RLIMIT_AS, (limit, limit))

def main(args):
    indexer_main(args)

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
    parser.add_argument(
        '-log-level',
        dest='log_level',
        action='store',
        required=False,
        type=str,
        help="logging level"
    )
    args = parser.parse_args()
    memory_limit(args.memory_limit)
    try:
        if args.log_level != None:
            log.set_level(args.log_level)

        main(args)
    except MemoryError:
        sys.stderr.write('\n\nERROR: Memory Exception\n')
        sys.exit(1)
    except Exception as e:
        logger.critical(f"Encountered fatal error: {e}", exc_info=True)
