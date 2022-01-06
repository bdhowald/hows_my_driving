import pytz
import re
import tweepy

from datetime import datetime, timezone

from traffic_violations.constants import (lookup_sources,
    regexps as regexp_constants, twitter as twitter_constants)
from traffic_violations.models.twitter_event import TwitterEvent


class BaseLookupRequest:

    def __init__(self, message_source: str):
        self.message_source: str = message_source

        # need to convert times to utc
        self.utc = pytz.timezone('UTC')

    def external_id(self):
        return self.id

    def is_complete_request(self):
        return all(getattr(self, attr) is not None for attr in ('created_at', 'id', 'legacy_string_parts', 'needs_reply', 'string_parts', 'username'))

    def is_direct_message(self):
        return self.message_source == lookup_sources.LookupSource.DIRECT_MESSAGE.value

    def is_status(self):
        return self.message_source == lookup_sources.LookupSource.STATUS.value

    def legacy_string_tokens(self):
        return self.legacy_string_parts

    def requesting_user_is_follower(self, follower_ids: list[int]):
        if not follower_ids:
            return True

        if not self.user_id:
            return True

        return self.user_id in follower_ids

    def requires_response(self):
        return self.needs_reply

    def string_tokens(self):
        return self.string_parts

    def username(self):
        return re.sub('@', '', self.user_handle)


class AccountActivityAPIDirectMessage(BaseLookupRequest):
    def __init__(self, message: TwitterEvent, message_source: str):
        super().__init__(message_source)

        text = message.event_text
        modified_string = ' '.join(text.split())

        self.created_at: datetime.datetime = self.utc.localize(datetime.utcfromtimestamp(
            (int(message.created_at) / 1000))).astimezone(timezone.utc).strftime(twitter_constants.TWITTER_TIME_FORMAT)
        self.id: str = message.event_id
        self.legacy_string_parts: list[str] = re.split(
            regexp_constants.LEGACY_STRING_PARTS_REGEX, modified_string.lower())
        self.mentioned_user_ids: list[str] = re.split(
            ',', message.user_mention_ids) if message.user_mention_ids is not None else []
        self.mentioned_users: list[str] = re.split(
            ' ', message.user_mentions) if message.user_mentions is not None else []
        self.needs_reply: bool = message.user_handle != twitter_constants.HMDNY_TWITTER_HANDLE
        self.string_parts: list[str] = re.split(' ', modified_string.lower())
        self.user_id: int = int(message.user_id)
        self.user_handle: str = '@' + message.user_handle


class AccountActivityAPIStatus(BaseLookupRequest):
    def __init__(self, message: TwitterEvent, message_source: str):
        super().__init__(message_source)

        text = message.event_text
        modified_string: str = ' '.join(text.split())

        self.created_at: datetime.datetime = self.utc.localize(
            datetime.utcfromtimestamp((int(message.created_at) / 1000))
                ).astimezone(timezone.utc).strftime(
                    twitter_constants.TWITTER_TIME_FORMAT)

        self.id: int = message.event_id
        self.legacy_string_parts: list[str] = re.split(
            regexp_constants.LEGACY_STRING_PARTS_REGEX, modified_string.lower())
        self.mentioned_user_ids: list[str] = re.split(
            ',', message.user_mention_ids) if message.user_mention_ids is not None else []
        self.mentioned_users: list[str] = re.split(
            ' ', message.user_mentions) if message.user_mentions is not None else []
        self.needs_reply: bool = message.user_handle != twitter_constants.HMDNY_TWITTER_HANDLE
        self.string_parts: list[str] = re.split(' ', modified_string.lower())
        self.user_id: int = int(message.user_id)
        self.user_handle: str = '@' + message.user_handle


class DirectMessageAPIDirectMessage(BaseLookupRequest):
    def __init__(self, message, message_source, api: tweepy.API):
        super().__init__(message_source)

        direct_message = message

        recipient_id = int(direct_message.message_create[
                           'target']['recipient_id'])
        sender_id = int(direct_message.message_create['sender_id'])

        recipient = api.get_user(recipient_id)
        sender = api.get_user(sender_id)

        if recipient.screen_name == twitter_constants.HMDNY_TWITTER_HANDLE:
            text = direct_message.message_create['message_data']['text']
            modified_string = ' '.join(text.split())

            self.created_at: datetime.datetime = self.utc.localize(
                datetime.utcfromtimestamp(
                    (int(direct_message.created_timestamp) / 1000))).astimezone(
                timezone.utc).strftime(twitter_constants.TWITTER_TIME_FORMAT)
            self.id: str = int(direct_message.id)
            self.legacy_string_parts: list[str] = re.split(
                regexp_constants.LEGACY_STRING_PARTS_REGEX,
                modified_string.lower())
            self.mentioned_users = []
            self.needs_reply: bool = sender.screen_name != twitter_constants.HMDNY_TWITTER_HANDLE
            self.string_parts: list[str] = re.split(
                ' ', modified_string.lower())
            self.user_id: int = int(sender.id)
            self.user_handle: str = '@' + sender.screen_name


class HowsMyDrivingAPIRequest(BaseLookupRequest):

    def __init__(self, message: dict, message_source: str):
        super().__init__(message_source)

        text = message['event_text']
        modified_string = ' '.join(text.split())

        self.created_at: datetime.datetime = message['created_at']
        self.id: str = message['event_id']
        self.legacy_string_parts: list[str] = re.split(
            regexp_constants.LEGACY_STRING_PARTS_REGEX, modified_string.lower())
        self.mentioned_user_ids = []
        self.mentioned_users = []
        self.needs_reply: bool = True
        self.string_parts: list[str] = re.split(
            ' ', modified_string.lower())
        self.user_handle: str = message['username']

        self.user_is_follower: bool = True


class SearchStatus(BaseLookupRequest):

    def __init__(self, message, message_source: str):
        super().__init__(message_source)

        entities = message.entities

        if 'user_mentions' in entities:
            array_of_user_ids = [v['id']
                                  for v in entities['user_mentions']]
            array_of_usernames = [v['screen_name']
                                  for v in entities['user_mentions']]

            if twitter_constants.HMDNY_TWITTER_HANDLE in array_of_usernames:
                full_text = message.full_text
                modified_string = ' '.join(full_text.split())

                self.created_at: datetime.datetime = self.utc.localize(
                    message.created_at).astimezone(
                        timezone.utc).strftime(
                            twitter_constants.TWITTER_TIME_FORMAT)
                self.id: str = message.id
                self.is_retweet: bool = hasattr( message, 'retweeted_status')
                self.legacy_string_parts: list[str] = re.split(
                    regexp_constants.LEGACY_STRING_PARTS_REGEX, modified_string.lower())
                self.mentioned_user_ids: list[str] = array_of_user_ids
                self.mentioned_users: list[str] = [s.lower()
                                                     for s in array_of_usernames]
                self.needs_reply: bool = message.user.screen_name != twitter_constants.HMDNY_TWITTER_HANDLE
                self.string_parts: list[str] = re.split(
                    ' ', modified_string.lower())
                self.user_id: int = int(message.user.id)
                self.user_handle: str = '@' + message.user.screen_name


class StreamExtendedStatus(BaseLookupRequest):

    def __init__(self, message, message_source: str):
        super().__init__(message_source)

        extended_tweet = message.extended_tweet

        # don't perform if there is no text
        if 'full_text' in extended_tweet:
            entities = extended_tweet['entities']

            if 'user_mentions' in entities:
                array_of_user_ids = [v['id']
                                  for v in entities['user_mentions']]
                array_of_usernames = [v['screen_name']
                                      for v in entities['user_mentions']]

                if twitter_constants.HMDNY_TWITTER_HANDLE in array_of_usernames:
                    full_text = extended_tweet['full_text']
                    modified_string = ' '.join(full_text.split())

                    self.created_at: datetime.datetime = self.utc.localize(message.created_at).astimezone(
                        timezone.utc).strftime(twitter_constants.TWITTER_TIME_FORMAT)
                    self.id: str = message.id
                    self.legacy_string_parts: list[str] = re.split(
                        regexp_constants.LEGACY_STRING_PARTS_REGEX, modified_string.lower())
                    self.mentioned_user_ids: list[str] = array_of_user_ids
                    self.mentioned_users: list[str] = [
                        s.lower() for s in array_of_usernames]
                    self.needs_reply: bool = message.user.screen_name != twitter_constants.HMDNY_TWITTER_HANDLE
                    self.string_parts: list[str] = re.split(
                        ' ', modified_string.lower())
                    self.user_id: int = int(message.user.id)
                    self.user_handle: str = '@' + message.user.screen_name


class StreamingDirectMessage(BaseLookupRequest):

    def __init__(self, message, message_source: str):
        super().__init__(message_source)

        direct_message = message.direct_message
        recipient = direct_message['recipient']
        sender = direct_message['sender']

        if recipient['screen_name'] == twitter_constants.HMDNY_TWITTER_HANDLE:
            text = direct_message['text']
            modified_string = ' '.join(text.split())

            self.created_at: datetime.datetime = direct_message['created_at']
            self.id: str = direct_message['id']
            self.legacy_string_parts: list[str] = re.split(
                regexp_constants.LEGACY_STRING_PARTS_REGEX, modified_string.lower())
            self.mentioned_user_ids = []
            self.mentioned_users = []
            self.needs_reply: bool = sender[
                'screen_name'] != twitter_constants.HMDNY_TWITTER_HANDLE
            self.string_parts: list[str] = re.split(
                ' ', modified_string.lower())
            self.user_id: int = int(sender['id'])
            self.user_handle: str = '@' + sender['screen_name']


class StreamingStatus(BaseLookupRequest):

    def __init__(self, message, message_source: str):
        super().__init__(message_source)

        entities = message.entities

        if 'user_mentions' in entities:
            array_of_user_ids = [v['id']
                                  for v in entities['user_mentions']]
            array_of_usernames = [v['screen_name']
                                  for v in entities['user_mentions']]

            if twitter_constants.HMDNY_TWITTER_HANDLE in array_of_usernames:
                text = message.text
                modified_string = ' '.join(text.split())

                self.created_at: datetime.datetime = self.utc.localize(message.created_at).astimezone(
                    timezone.utc).strftime(twitter_constants.TWITTER_TIME_FORMAT)
                self.id: str = message.id
                self.is_retweet: bool = hasattr(
                    message, 'retweeted_status')
                self.legacy_string_parts: list[str] = re.split(
                    regexp_constants.LEGACY_STRING_PARTS_REGEX, modified_string.lower())
                self.mentioned_user_ids: list[str] = array_of_user_ids
                self.mentioned_users: list[str] = [s.lower()
                                                     for s in array_of_usernames]
                self.needs_reply: bool = message.user.screen_name != twitter_constants.HMDNY_TWITTER_HANDLE
                self.string_parts: list[str] = re.split(
                    ' ', modified_string.lower())
                self.user_id: int = int(message.user.id)
                self.user_handle: str = '@' + message.user.screen_name
