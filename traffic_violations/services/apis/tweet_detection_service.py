import re
import requests
import requests_futures.sessions

from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

from typing import Any, Dict


class TweetDetectionService:

    def __init__(self):
        # Set up retry ability
        s_req = requests_futures.sessions.FuturesSession(max_workers=5)

        retries = Retry(total=5,
                        backoff_factor=0.1,
                        status_forcelist=[403, 500, 502, 503, 504],
                        raise_on_status=False)

        s_req.mount('https://', HTTPAdapter(max_retries=retries))
        self.api = s_req


    def tweet_exists(self, id: int, username: str) -> bool:
        result = self._perform_query(f'https://twitter.com/{username}/status/{str(id)}')

        return re.search('errorpage-body-content', result.content.decode("utf-8")) is None


    def _perform_query(self, url: str) -> Dict[str, Any]:
        response = self.api.get(url, stream=True)

        return response.result()
