import logging
import optparse
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
    print('Setting up logging')
    parser = optparse.OptionParser()
    parser.add_option('-l', '--logging-level', help='Logging level')
    parser.add_option('-f', '--logging-file', help='Logging file name')
    (options, args) = parser.parse_args()
    logging_level = LOGGING_LEVELS.get(
        options.logging_level, logging.NOTSET)
    logging.basicConfig(level=logging_level, filename=options.logging_file,
                        format='%(asctime)s %(levelname)s: %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    tweeter = TrafficViolationsTweeter()

    if sys.argv[-1] == 'print_daily_summary':
        tweeter._print_daily_summary()
    elif sys.argv[-1] == 'print_featured_plate':
        tweeter._print_featured_plate()
    else:
        tweeter._find_and_respond_to_twitter_events()

if __name__ == '__main__':
    run()