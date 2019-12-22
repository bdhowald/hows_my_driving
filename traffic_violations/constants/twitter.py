from enum import Enum

HMDNY_TWITTER_HANDLE: str = 'HowsMyDrivingNY'
MAX_TWITTER_STATUS_LENGTH: int = 280
TWITTER_TIME_FORMAT: str = '%a %b %d %H:%M:%S %z %Y'

class TwitterMessageTypes(Enum):
    DIRECT_MESSAGE = 'direct_message'
    STATUS = 'status'