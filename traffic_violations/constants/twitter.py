from enum import Enum

HMDNY_TWITTER_HANDLE: str = 'HowsMyDrivingNY'
MAX_TWITTER_STATUS_LENGTH: int = 280
TWITTER_TIME_FORMAT: str = '%a %b %d %H:%M:%S %z %Y'

class TwitterAPIAttributes(Enum):
    DIRECT_MESSAGE = 'direct_message'
    ENTITIES = 'entities'
    EXTENDED_TWEET = 'extended_tweet'
    EVENT_TYPE = 'event_type'
    FULL_TEXT = 'full_text'
    MESSAGE_CREATE = 'message_create'
    RETWEETED_STATUS = 'retweeted_status'

class TwitterMessageTypes(Enum):
    DIRECT_MESSAGE = 'direct_message'
    STATUS = 'status'