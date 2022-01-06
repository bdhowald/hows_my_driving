import json
import logging
import os
import tweepy

from traffic_violations import settings
from traffic_violations.constants.lookup_sources import LookupSource

LOG = logging.getLogger(__name__)


class TrafficViolationsStreamListener(tweepy.streaming.Stream):

    def __init__(self, tweeter):
        # Create a logger
        self.tweeter = tweeter

        self._app_api = None

        super().__init__(
            consumer_key=os.getenv('TWITTER_API_KEY'),
            consumer_secret=os.getenv('TWITTER_API_SECRET'),
            access_token=os.getenv('TWITTER_ACCESS_TOKEN'),
            access_token_secret=os.getenv('TWITTER_ACCESS_TOKEN_SECRET')
        )

    def on_status(self, status):
        LOG.debug(f'on_status: {status.text}')

    def on_data(self, data):
        data_dict: dict[any, any] = json.loads(data)
        formatted_data = json.dumps(data_dict,
                                    indent=4,
                                    sort_keys=True)

        LOG.debug(f'data: {formatted_data}')

        if 'delete' in data_dict:
            LOG.debug(
                f"data_dict['delete']: {data_dict['delete']}")

        elif 'event' in data_dict:
            LOG.debug(
                f"data_dict['event']: {data_dict['event']}")

            status = tweepy.models.Status.parse(None, data_dict)

        elif 'direct_message' in data_dict:
            LOG.debug(
                f"data_dict['direct_message']: {data_dict['direct_message']}")

            message = tweepy.models.Status.parse(None, data_dict)

            self.tweeter.aggregator.initiate_reply(message, LookupSource.DIRECT_MESSAGE.value)

        elif 'friends' in data_dict:
            LOG.debug(
                f"data_dict['friends']: {data_dict['friends']}")

        elif 'limit' in data_dict:
            LOG.debug(
                f"data_dict['limit']: {data_dict['limit']}")

        elif 'disconnect' in data_dict:
            LOG.debug(
                f"data_dict['disconnect']: {data_dict['disconnect']}")

        elif 'warning' in data_dict:
            LOG.debug(
                f"data_dict['warning']: {data_dict['warning']}")

        elif 'retweeted_status' in data_dict:
            LOG.debug(f"is_retweet: {'retweeted_status' in data_dict}")
            LOG.debug(
                f"data_dict['retweeted_status']: "
                f"{data_dict['retweeted_status']}")

        elif 'in_reply_to_status_id' in data_dict:
            LOG.debug(
                f"data_dict['in_reply_to_status_id']: "
                f"{data_dict['in_reply_to_status_id']}")

            status = tweepy.models.Status.parse(None, data_dict)

            self.tweeter.aggregator.initiate_reply(status, LookupSource.STATUS.value)

        else:
            LOG.error("Unknown message type: " + str(data))

    def on_event(self, status):
        LOG.debug(f'on_event: {status}')

    def on_error(self, status):
        LOG.debug(f'on_error: {status}')

    def on_direct_message(self, status):
        LOG.debug(f'on_direct_message: {status}')
