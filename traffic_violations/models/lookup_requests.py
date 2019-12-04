import pytz
import re

from datetime import datetime, timezone


class BaseLookupRequest:

    hmdny_twitter_handle = 'HowsMyDrivingNY'
    hmdny_twitter_id = 976593574732222465
    legacy_string_parts_regex = r'(?<!state:|plate:)\s'
    strftime_format_string = '%a %b %d %H:%M:%S %z %Y'

    def __init__(self, message_source, message_type):
        self.arguments = {
            'message_source': message_source,
            'message_type': message_type
        }

        # need to convert times to utc
        self.utc = pytz.timezone('UTC')

    def created_at(self):
        return self.arguments.get('created_at')

    def external_id(self):
        return self.arguments.get('id')

    def is_complete_request(self):
        return all(k in self.arguments for k in ('created_at', 'id', 'legacy_string_parts', 'needs_reply', 'string_parts', 'username'))

    def legacy_string_tokens(self):
        return self.arguments.get('legacy_string_parts')

    def mentioned_users(self):
        return self.arguments.get('mentioned_users') or []

    def message_source(self):
        return self.arguments.get('message_source')

    def message_type(self):
        return self.arguments.get('message_type')

    def requires_response(self):
        return self.arguments.get('needs_reply')

    def string_tokens(self):
        return self.arguments.get('string_parts')

    def user_id(self):
        return self.arguments.get('user_id')

    def username(self):
        return self.arguments.get('username')


class AccountActivityAPIDirectMessage(BaseLookupRequest):
    def __init__(self, message, message_source, message_type):
        BaseLookupRequest.__init__(self, message_source, message_type)

        text = message['event_text']
        modified_string = ' '.join(text.split())

        self.arguments['created_at'] = self.utc.localize(datetime.utcfromtimestamp(
            (int(message['created_at']) / 1000))).astimezone(timezone.utc).strftime(self.strftime_format_string)
        self.arguments['id'] = message['event_id']
        self.arguments['legacy_string_parts'] = re.split(
            self.legacy_string_parts_regex, modified_string.lower())
        self.arguments['mentioned_users'] = re.split(
            ' ', message['user_mentions']) if message.get('user_mentions') else []
        self.arguments['needs_reply'] = message[
            'user_handle'] != self.hmdny_twitter_handle
        self.arguments['string_parts'] = re.split(' ', modified_string.lower())
        self.arguments['user_id'] = message['user_id']
        self.arguments['username'] = '@' + message['user_handle']


class AccountActivityAPIStatus(BaseLookupRequest):
    def __init__(self, message, message_source, message_type):
        BaseLookupRequest.__init__(self, message_source, message_type)

        text = message['event_text']
        modified_string = ' '.join(text.split())

        self.arguments['created_at'] = self.utc.localize(datetime.utcfromtimestamp(
            (int(message['created_at']) / 1000))).astimezone(timezone.utc).strftime(self.strftime_format_string)
        self.arguments['id'] = message['event_id']
        self.arguments['legacy_string_parts'] = re.split(
            self.legacy_string_parts_regex, modified_string.lower())
        self.arguments['mentioned_users'] = re.split(
            ' ', message['user_mentions']) if message.get('user_mentions') else []
        self.arguments['needs_reply'] = message[
            'user_handle'] != self.hmdny_twitter_handle
        self.arguments['string_parts'] = re.split(' ', modified_string.lower())
        self.arguments['user_id'] = message['user_id']
        self.arguments['username'] = '@' + message['user_handle']


class DirectMessageAPIDirectMessage(BaseLookupRequest):
    def __init__(self, message, message_source, message_type, api):
        BaseLookupRequest.__init__(self, message_source, message_type)

        direct_message = message

        recipient_id = int(direct_message.message_create[
                           'target']['recipient_id'])
        sender_id = int(direct_message.message_create['sender_id'])

        recipient = api.get_user(recipient_id)
        sender = api.get_user(sender_id)

        if recipient.screen_name == self.hmdny_twitter_handle:
            text = direct_message.message_create['message_data']['text']
            modified_string = ' '.join(text.split())

            self.arguments['created_at'] = self.utc.localize(datetime.utcfromtimestamp(
                (int(direct_message.created_timestamp) / 1000))).astimezone(timezone.utc).strftime(self.strftime_format_string)
            self.arguments['id'] = int(direct_message.id)
            self.arguments['legacy_string_parts'] = re.split(
                self.legacy_string_parts_regex, modified_string.lower())
            self.arguments[
                'needs_reply'] = sender.screen_name != self.hmdny_twitter_handle
            self.arguments['string_parts'] = re.split(
                ' ', modified_string.lower())
            self.arguments['user_id'] = sender.id
            self.arguments['username'] = '@' + sender.screen_name


class HowsMyDrivingAPIRequest(BaseLookupRequest):

    def __init__(self, message, message_source, message_type):
        BaseLookupRequest.__init__(self, message_source, message_type)

        text = message['event_text']
        modified_string = ' '.join(text.split())

        self.arguments['created_at'] = message['created_at']
        self.arguments['id'] = message['event_id']
        self.arguments['legacy_string_parts'] = re.split(
            self.legacy_string_parts_regex, modified_string.lower())
        self.arguments['mentioned_users'] = []
        self.arguments['needs_reply'] = True
        self.arguments['string_parts'] = re.split(
            ' ', modified_string.lower())
        self.arguments['username'] = message['username']


class SearchStatus(BaseLookupRequest):

    def __init__(self, message, message_source, message_type):
        BaseLookupRequest.__init__(self, message_source, message_type)

        entities = message.entities

        if 'user_mentions' in entities:
            array_of_usernames = [v['screen_name']
                                  for v in entities['user_mentions']]

            if self.hmdny_twitter_handle in array_of_usernames:
                full_text = message.full_text
                modified_string = ' '.join(full_text.split())

                self.arguments['created_at'] = self.utc.localize(message.created_at).astimezone(
                    timezone.utc).strftime(self.strftime_format_string)
                self.arguments['id'] = message.id
                self.arguments['is_retweet'] = hasattr(
                    message, 'retweeted_status')
                self.arguments['legacy_string_parts'] = re.split(
                    self.legacy_string_parts_regex, modified_string.lower())
                self.arguments['mentioned_users'] = [s.lower()
                                                     for s in array_of_usernames]
                self.arguments[
                    'needs_reply'] = message.user.screen_name != self.hmdny_twitter_handle
                self.arguments['string_parts'] = re.split(
                    ' ', modified_string.lower())
                self.arguments['user_id'] = message.user.id
                self.arguments['username'] = '@' + message.user.screen_name



class StreamExtendedStatus(BaseLookupRequest):

    def __init__(self, message, message_source, message_type):
        BaseLookupRequest.__init__(self, message_source, message_type)

        extended_tweet = message.extended_tweet

        # don't perform if there is no text
        if 'full_text' in extended_tweet:
            entities = extended_tweet['entities']

            if 'user_mentions' in entities:
                array_of_usernames = [v['screen_name']
                                      for v in entities['user_mentions']]

                if self.hmdny_twitter_handle in array_of_usernames:
                    full_text = extended_tweet['full_text']
                    modified_string = ' '.join(full_text.split())

                    self.arguments['created_at'] = self.utc.localize(message.created_at).astimezone(
                        timezone.utc).strftime(self.strftime_format_string)
                    self.arguments['id'] = message.id
                    self.arguments['legacy_string_parts'] = re.split(
                        self.legacy_string_parts_regex, modified_string.lower())
                    self.arguments['mentioned_users'] = [
                        s.lower() for s in array_of_usernames]
                    self.arguments[
                        'needs_reply'] = message.user.screen_name != self.hmdny_twitter_handle
                    self.arguments['string_parts'] = re.split(
                        ' ', modified_string.lower())
                    self.arguments['user_id'] = message.user.id
                    self.arguments['username'] = '@' + message.user.screen_name


class StreamingDirectMessage(BaseLookupRequest):

    def __init__(self, message, message_source, message_type):
        BaseLookupRequest.__init__(self, message_source, message_type)

        direct_message = message.direct_message
        recipient = direct_message['recipient']
        sender = direct_message['sender']

        if recipient['screen_name'] == self.hmdny_twitter_handle:
            text = direct_message['text']
            modified_string = ' '.join(text.split())

            self.arguments['created_at'] = direct_message['created_at']
            self.arguments['id'] = direct_message['id']
            self.arguments['legacy_string_parts'] = re.split(
                self.legacy_string_parts_regex, modified_string.lower())
            self.arguments['needs_reply'] = sender[
                'screen_name'] != self.hmdny_twitter_handle
            self.arguments['string_parts'] = re.split(
                ' ', modified_string.lower())
            self.arguments['user_id'] = sender['id']
            self.arguments['username'] = '@' + sender['screen_name']


class StreamingStatus(BaseLookupRequest):

    def __init__(self, message, message_source, message_type):
        BaseLookupRequest.__init__(self, message_source, message_type)

        entities = message.entities

        if 'user_mentions' in entities:
            array_of_usernames = [v['screen_name']
                                  for v in entities['user_mentions']]

            if self.hmdny_twitter_handle in array_of_usernames:
                text = message.text
                modified_string = ' '.join(text.split())

                self.arguments['created_at'] = self.utc.localize(message.created_at).astimezone(
                    timezone.utc).strftime(self.strftime_format_string)
                self.arguments['id'] = message.id
                self.arguments['is_retweet'] = hasattr(
                    message, 'retweeted_status')
                self.arguments['legacy_string_parts'] = re.split(
                    self.legacy_string_parts_regex, modified_string.lower())
                self.arguments['mentioned_users'] = [s.lower()
                                                     for s in array_of_usernames]
                self.arguments[
                    'needs_reply'] = message.user.screen_name != self.hmdny_twitter_handle
                self.arguments['string_parts'] = re.split(
                    ' ', modified_string.lower())
                self.arguments['user_id'] = message.user.id
                self.arguments['username'] = '@' + message.user.screen_name
