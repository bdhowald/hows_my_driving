import logging
import pytz
import re
import pdb

from datetime import datetime, timezone, time, timedelta



class ReplyArgumentBuilder:

    _logger                   = logging.getLogger('hows_my_driving')
    legacy_string_parts_regex = r'(?<!state:|plate:)\s'
    hmdny_twitter_handle      = 'HowsMyDrivingNY'
    strftime_format_string    = '%a %b %d %H:%M:%S %z %Y'


    @classmethod
    def build_reply_data(cls, message, message_source, message_type):

        # Print args
        cls._logger.info('args:')
        cls._logger.info('message: %s', message)
        cls._logger.info('message_source: %s', message_source)
        cls._logger.info('message_type: %s', message_type)

        # dict to store necessary parts for response
        args_for_response = {
          'source': message_source,
          'type'  : message_type
        }

        # need to convert times to utc
        utc = pytz.timezone('UTC')


        # why doesn't python have switch statements
        if message_source == "twitter":

            if message_type == 'status':

                # Using old streaming service for a tweet longer than 140 characters

                if hasattr(message, 'extended_tweet'):
                    cls._logger.debug('\n\nWe have an extended tweet\n\n')

                    extended_tweet = message.extended_tweet

                    # don't perform if there is no text
                    if 'full_text' in extended_tweet:
                        entities = extended_tweet['entities']

                        if 'user_mentions' in entities:
                            array_of_usernames = [v['screen_name'] for v in entities['user_mentions']]

                            if 'HowsMyDrivingNY' in array_of_usernames:
                                full_text       = extended_tweet['full_text']
                                modified_string = ' '.join(full_text.split())

                                args_for_response['created_at']          = utc.localize(message.created_at).astimezone(timezone.utc).strftime(cls.strftime_format_string)
                                args_for_response['id']                  = message.id
                                args_for_response['legacy_string_parts'] = re.split(cls.legacy_string_parts_regex, modified_string.lower())
                                args_for_response['mentioned_users']     = [s.lower() for s in array_of_usernames]
                                args_for_response['needs_reply']         = message.user.screen_name != cls.hmdny_twitter_handle
                                args_for_response['string_parts']        = re.split(' ', modified_string.lower())
                                args_for_response['user_id']             = message.user.id
                                args_for_response['username']            = '@' + message.user.screen_name


                # Using tweet api search endpoint

                elif hasattr(message, 'full_text') and (not hasattr(message, 'retweeted_status')):
                    cls._logger.debug('\n\nWe have a tweet from the search api endpoint\n\n')

                    entities = message.entities

                    if 'user_mentions' in entities:
                        array_of_usernames = [v['screen_name'] for v in entities['user_mentions']]

                        if cls.hmdny_twitter_handle in array_of_usernames:
                            full_text       = message.full_text
                            modified_string = ' '.join(full_text.split())

                            args_for_response['created_at']          = utc.localize(message.created_at).astimezone(timezone.utc).strftime(cls.strftime_format_string)
                            args_for_response['id']                  = message.id
                            args_for_response['legacy_string_parts'] = re.split(cls.legacy_string_parts_regex, modified_string.lower())
                            args_for_response['mentioned_users']     = [s.lower() for s in array_of_usernames]
                            args_for_response['needs_reply']         = message.user.screen_name != cls.hmdny_twitter_handle
                            args_for_response['string_parts']        = re.split(' ', modified_string.lower())
                            args_for_response['user_id']             = message.user.id
                            args_for_response['username']            = '@' + message.user.screen_name


                # Using old streaming service for a tweet of 140 characters or fewer

                elif hasattr(message, 'entities') and (not hasattr(message, 'retweeted_status')):

                    cls._logger.debug('\n\nWe are dealing with a tweet of 140 characters or fewer\n\n')

                    entities = message.entities

                    if 'user_mentions' in entities:
                        array_of_usernames = [v['screen_name'] for v in entities['user_mentions']]

                        if cls.hmdny_twitter_handle in array_of_usernames:
                            text            = message.text
                            modified_string = ' '.join(text.split())

                            args_for_response['created_at']          = utc.localize(message.created_at).astimezone(timezone.utc).strftime(cls.strftime_format_string)
                            args_for_response['id']                  = message.id
                            args_for_response['legacy_string_parts'] = re.split(cls.legacy_string_parts_regex, modified_string.lower())
                            args_for_response['mentioned_users']     = [s.lower() for s in array_of_usernames]
                            args_for_response['needs_reply']         = message.user.screen_name != cls.hmdny_twitter_handle
                            args_for_response['string_parts']        = re.split(' ', modified_string.lower())
                            args_for_response['user_id']             = message.user.id
                            args_for_response['username']            = '@' + message.user.screen_name


                # Using new account api service by way of SQL table for events

                elif type(message) == dict and 'event_type' in message:

                    cls._logger.debug('\n\nWe are dealing with account activity api object\n\n')

                    text            = message['event_text']
                    modified_string = ' '.join(text.split())

                    args_for_response['created_at']          = utc.localize(datetime.utcfromtimestamp((int(message['created_at']) / 1000))).astimezone(timezone.utc).strftime(cls.strftime_format_string)
                    args_for_response['id']                  = message['event_id']
                    args_for_response['legacy_string_parts'] = re.split(cls.legacy_string_parts_regex, modified_string.lower())
                    args_for_response['mentioned_users']     = re.split(' ', message['user_mentions']) if message['user_mentions'] is not None else []
                    args_for_response['needs_reply']         = message['user_handle'] != cls.hmdny_twitter_handle
                    args_for_response['string_parts']        = re.split(' ', modified_string.lower())
                    args_for_response['user_id']             = message['user_id']
                    args_for_response['username']            = '@' + message['user_handle']


            elif message_type == 'direct_message':

                cls._logger.debug('\n\nWe have a direct message\n\n')


                # Using old streaming service for a direct message

                if hasattr(message, 'direct_message'):

                    direct_message  = message.direct_message
                    recipient       = direct_message['recipient']
                    sender          = direct_message['sender']

                    if recipient['screen_name'] == cls.hmdny_twitter_handle:
                        text            = direct_message['text']
                        modified_string = ' '.join(text.split())

                        args_for_response['created_at']          = direct_message['created_at']
                        args_for_response['id']                  = direct_message['id']
                        args_for_response['legacy_string_parts'] = re.split(cls.legacy_string_parts_regex, modified_string.lower())
                        args_for_response['needs_reply']         = sender['screen_name'] != cls.hmdny_twitter_handle
                        args_for_response['string_parts']        = re.split(' ', modified_string.lower())
                        args_for_response['user_id']             = sender['id']
                        args_for_response['username']            = '@' + sender['screen_name']



                # Using new direct message api endpoint

                elif hasattr(message, 'message_create'):

                    direct_message  = message

                    recipient_id    = int(direct_message.message_create['target']['recipient_id'])
                    sender_id       = int(direct_message.message_create['sender_id'])

                    recipient       = self.api.get_user(recipient_id)
                    sender          = self.api.get_user(sender_id)

                    if recipient.screen_name == cls.hmdny_twitter_handle:
                        text            = direct_message.message_create['message_data']['text']
                        modified_string = ' '.join(text.split())

                        args_for_response['created_at']          = utc.localize(datetime.utcfromtimestamp((int(direct_message.created_timestamp) / 1000))).astimezone(timezone.utc).strftime(cls.strftime_format_string)
                        args_for_response['id']                  = int(direct_message.id)
                        args_for_response['legacy_string_parts'] = re.split(cls.legacy_string_parts_regex, modified_string.lower())
                        args_for_response['needs_reply']         = sender.screen_name != cls.hmdny_twitter_handle
                        args_for_response['string_parts']        = re.split(' ', modified_string.lower())
                        args_for_response['user_id']             = sender.id
                        args_for_response['username']            = '@' + sender.screen_name


                # Using account activity api endpoint

                elif 'event_type' in message:

                    cls._logger.debug('\n\nWe are dealing with account activity api object\n\n')

                    text            = message['event_text']
                    modified_string = ' '.join(text.split())

                    args_for_response['created_at']          = utc.localize(datetime.utcfromtimestamp((int(message['created_at']) / 1000))).astimezone(timezone.utc).strftime(cls.strftime_format_string)
                    args_for_response['id']                  = message['event_id']
                    args_for_response['legacy_string_parts'] = re.split(cls.legacy_string_parts_regex, modified_string.lower())
                    args_for_response['mentioned_users']     = re.split(' ', message['user_mentions']) if message['user_mentions'] is not None else []
                    args_for_response['needs_reply']         = message['user_handle'] != cls.hmdny_twitter_handle
                    args_for_response['string_parts']        = re.split(' ', modified_string.lower())
                    args_for_response['user_id']             = message['user_id']
                    args_for_response['username']            = '@' + message['user_handle']


        elif message_source == 'api':

            text            = message['event_text']
            modified_string = ' '.join(text.split())

            args_for_response['created_at']          = message['created_at']
            args_for_response['id']                  = message['event_id']
            args_for_response['legacy_string_parts'] = re.split(cls.legacy_string_parts_regex, modified_string.lower())
            args_for_response['mentioned_users']     = []
            args_for_response['needs_reply']         = True
            args_for_response['string_parts']        = re.split(' ', modified_string.lower())
            args_for_response['username']            = message['username']


        else:
            # Do something
            1+1


        return args_for_response

