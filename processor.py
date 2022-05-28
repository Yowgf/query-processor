import argparse

from processor.main import main as processor_main
from common.log import log

logger = log.logger()

class InvalidConfigError(Exception):
    def __init__(self, config_option_key, err_msg):
        super().__init__(
            "invalid value for flag '{}': {}".format(
                config_option_key, err_msg,
            )
        )

def parse_args():
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument(
        '-i',
        dest='index_file',
        action='store',
        required=True,
        type=str,
        help='path to index file'
    )
    parser.add_argument(
        '-q',
        dest='queries',
        action='store',
        required=True,
        type=str,
        help='path to a file with a list of queries to process'
    )
    parser.add_argument(
        '-r',
        dest='ranker',
        action='store',
        required=True,
        type=str,
        help="['TFIDF' | 'BM25'] ranking function to score documents with"
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
    return args

def main():
    try:
        args = parse_args()
        if args.log_level != None:
            log.set_level(args.log_level)

        processor_main(args)

    except Exception as e:
        logger.critical(f"Encountered fatal error: "+
                     "{e}", exc_info=True)

if __name__ == "__main__":
    main()
