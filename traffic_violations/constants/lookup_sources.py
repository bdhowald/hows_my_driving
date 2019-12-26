from enum import Enum

class LookupSource(Enum):
    API = 'api'
    DIRECT_MESSAGE = 'direct_message'
    EXTERNAL = 'external'
    STATUS = 'status'
    WEB = 'web_client'
