import abc
import os
import tweepy

from traffic_violations import settings
from traffic_violations.constants import environment


class TwitterApiWrapper(abc.ABC):
    def __init__(self, 
                 api_key: str,
                 api_secret: str,
                 access_token: str,
                 access_token_secret: str
    ):
        auth = tweepy.OAuthHandler(api_key, api_secret)
        auth.set_access_token(access_token, access_token_secret)
  
        self._connection = tweepy.API(auth,
                                      wait_on_rate_limit=True,
                                      retry_count=3,
                                      retry_delay=5,
                                      retry_errors=set([403, 500, 503])
        )

    def get_connection(self):
        return self._connection


class TwitterApplicationApiWrapper(TwitterApiWrapper):
    def __init__(self):
        super().__init__(
          api_key=os.getenv(
              environment.EnvrionmentVariable.TWITTER_API_KEY.value
          ),
          api_secret=os.getenv(
              environment.EnvrionmentVariable.TWITTER_API_SECRET.value
          ),
          access_token=os.getenv(
              environment.EnvrionmentVariable.TWITTER_ACCESS_TOKEN.value
          ),
          access_token_secret=os.getenv(
              environment.EnvrionmentVariable.TWITTER_ACCESS_TOKEN_SECRET.value
          )
        )


class TwitterClientApiWrapper(TwitterApiWrapper):
    def __init__(self):
        super().__init__(
          api_key=os.getenv(
              environment.EnvrionmentVariable.TWITTER_API_KEY.value
          ),
          api_secret=os.getenv(
              environment.EnvrionmentVariable.TWITTER_API_SECRET.value
          ),
          access_token=os.getenv(
              environment.EnvrionmentVariable.TWITTER_CLIENT_ACCESS_TOKEN.value
          ),
          access_token_secret=os.getenv(
              environment.EnvrionmentVariable.TWITTER_CLIENT_ACCESS_TOKEN_SECRET.value
          )
        )
