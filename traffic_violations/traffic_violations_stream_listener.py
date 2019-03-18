import json
import logging
import tweepy


class TrafficViolationsStreamListener (tweepy.StreamListener):

    def __init__(self, tweeter):
        # Create a logger
        self.logger = logging.getLogger('hows_my_driving')
        self.tweeter = tweeter

        super(TrafficViolationsStreamListener, self).__init__()

    def on_status(self, status):
        self.logger.debug("\n\n\non_status: %s\n\n\n", status.text)

    def on_data(self, data):
        data_dict = json.loads(data)
        self.logger.debug("\n\ndata: %s\n\n", json.dumps(
            data_dict, indent=4, sort_keys=True))

        if 'delete' in data_dict:
            self.logger.debug('\n\ndelete\n')
            self.logger.debug(
                "\ndata_dict['delete']: %s\n\n", data_dict['delete'])
            # delete = data['delete']['status']

            # if self.on_delete(delete['id'], delete['user_id']) is False:
            #     return False
        elif 'event' in data_dict:
            self.logger.debug('\n\nevent\n')
            self.logger.debug(
                "\ndata_dict['event']: %s\n\n", data_dict['event'])

            status = tweepy.Status.parse(self.api, data_dict)
            # if self.on_event(status) is False:
            #     return False
        elif 'direct_message' in data_dict:
            self.logger.debug('\n\ndirect_message\n')
            self.logger.debug("\ndata_dict['direct_message']: %s\n\n", data_dict[
                              'direct_message'])

            message = tweepy.Status.parse(self.api, data_dict)

            self.tweeter.aggregator.initiate_reply(message, 'direct_message')
            # if self.on_direct_message(status) is False:
            #     return False
        elif 'friends' in data_dict:
            self.logger.debug('\n\nfriends\n')
            self.logger.debug(
                "\ndata_dict['friends']: %s\n\n", data_dict['friends'])

            # if self.on_friends(data['friends']) is False:
            #     return False
        elif 'limit' in data_dict:
            self.logger.debug('\n\nlimit\n')
            self.logger.debug(
                "\ndata_dict['limit']: %s\n\n", data_dict['limit'])

            # if self.on_limit(data['limit']['track']) is False:
            #     return False
        elif 'disconnect' in data_dict:
            self.logger.debug('\n\ndisconnect\n')
            self.logger.debug(
                "\ndata_dict['disconnect']: %s\n\n", data_dict['disconnect'])

            # if self.on_disconnect(data['disconnect']) is False:
            #     return False
        elif 'warning' in data_dict:
            self.logger.debug('\n\nwarning\n')
            self.logger.debug(
                "\ndata_dict['warning']: %s\n\n", data_dict['warning'])

            # if self.on_warning(data['warning']) is False:
            #     return False
        elif 'retweeted_status' in data_dict:
            self.logger.debug("\n\nis_retweet: %s\n",
                              'retweeted_status' in data_dict)
            self.logger.debug("\ndata_dict['retweeted_status']: %s\n\n", data_dict[
                              'retweeted_status'])

        elif 'in_reply_to_status_id' in data_dict:
            self.logger.debug('\n\nin_reply_to_status_id\n')
            self.logger.debug("\ndata_dict['in_reply_to_status_id']: %s\n\n", data_dict[
                              'in_reply_to_status_id'])

            status = tweepy.Status.parse(self.api, data_dict)

            self.tweeter.aggregator.initiate_reply(status, 'status')
            # if self.on_status(status) is False:
            #     return False
        else:
            self.logger.error("Unknown message type: " + str(data))

    def on_event(self, status):
        self.logger.debug("on_event: %s", status)

    def on_error(self, status):
        self.logger.debug("on_error: %s", status)
        # self.logger.debug("self: %s", self)

    def on_direct_message(self, status):
        self.logger.debug("on_direct_message: %s", status)
