from enum import Enum

HMDNY_TWITTER_HANDLE: str = 'HowsMyDrivingNY'
HMDNY_TWITTER_USER_ID: int = 976593574732222465
MAX_TWITTER_STATUS_LENGTH: int = 280
TWITTER_TIME_FORMAT: str = '%a %b %d %H:%M:%S %z %Y'

class TwitterAPIAttribute(Enum):
    DIRECT_MESSAGE = 'direct_message'
    ENTITIES = 'entities'
    EXTENDED_TWEET = 'extended_tweet'
    EVENT_TYPE = 'event_type'
    FULL_TEXT = 'full_text'
    MESSAGE_CREATE = 'message_create'
    RETWEETED_STATUS = 'retweeted_status'

class TwitterMessageType(Enum):
    DIRECT_MESSAGE = 'direct_message'
    STATUS = 'status'