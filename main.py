import argparse
import logging
import sys

from traffic_violations.services.twitter_service import \
    TrafficViolationsTweeter

LOGGING_LEVELS = {'critical': logging.CRITICAL,
                  'error': logging.ERROR,
                  'warning': logging.WARNING,
                  'info': logging.INFO,
                  'debug': logging.DEBUG}

LOG = logging.getLogger(__name__)

def run():
    tweeter = TrafficViolationsTweeter()

    if sys.argv[-1] == 'print_daily_summary':
        tweeter._print_daily_summary()
    elif sys.argv[-1] == 'print_featured_plate':
        tweeter._print_featured_plate()
    else:
        tweeter._find_and_respond_to_twitter_events()

def parse_args():
    parser = argparse.ArgumentParser(
        description='Run HowsMyDrivingNY')
    parser.add_argument(
        '-l',
        '--log-level',
        help='Log level')
    parser.add_argument(
        '-f',
        '--log-file',
        help='Log file name')
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_args()

    log_level: str = args.log_level
    log_file = args.log_file

    logging_level: int = LOGGING_LEVELS.get(
        args.log_level, logging.NOTSET)
    logging.basicConfig(level=logging_level, filename=args.log_file,
                        format='%(asctime)s %(levelname)s: %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    run()