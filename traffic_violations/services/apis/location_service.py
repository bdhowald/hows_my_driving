import os
import requests

from common.db_service import DbService
from typing import Dict, List, Optional

class LocationService:

    GEOCODING_SERVICE_API_KEY = os.environ['GOOGLE_API_KEY'] if os.environ.get(
            'GOOGLE_API_KEY') else ''
    GEOCODING_SERVICE_ENDPOINT = 'https://maps.googleapis.com/maps/api/geocode/json'
    GEOCODING_SERVICE_NAME = 'google'


    def __init__(self, logger):
        self.logger = logger
        self.db_service = DbService(self.logger)


    def get_borough_from_location_strings(self, location_parts: List[str]) -> Optional[str]:
        return self._detect_borough(location_parts=location_parts)


    def _detect_borough(self, location_parts) -> Optional[str]:

        # sanitized_parts = [re.sub('\(?[ENSW]/?B\)? *', '', part)
        #                    for part in location_parts]
        location_strs: List[str] = self._normalize_address(location_parts=location_parts)

        # location_str = re.sub('[ENSW]B *', '', location_str)
        # lookup_string = ' '.join([location_str, 'New York NY'])

        for geo_string in location_strs:

            # try to find it in the geocodes table first.
            conn = self.db_service.get_connection()

            boro_from_geocode: Optional[str] = self._get_existing_geocode(db_conn=conn,
                query_string=geo_string)

            if boro_from_geocode:
                return boro_from_geocode[0]

            else:
                params: Dict[str, str] = {'address': geo_string, 'key': self.GEOCODING_SERVICE_API_KEY}
                results: Dict[str, str] = self._make_geocoding_request(params=params)

                if results:
                    boro: str = self._parse_geocoding_response_for_borough(response=results[0])

                    if boro:
                        # insert geocode
                        self._save_new_geocode(
                            db_conn=conn, borough=boro, query_string=geo_string)

                        # return the boro
                        return boro

            # Close the connection
            conn.close()

        return None


    def _get_existing_geocode(self, db_conn, query_string) -> None:
        db_conn.execute(""" select borough from geocodes where lookup_string = %s """, (query_string)).fetchone()


    def _make_geocoding_request(self, params) -> Dict[str, str]:
        req = requests.get(self.GEOCODING_SERVICE_ENDPOINT, params=params)

        return req.json()['results']


    def _normalize_address(self, location_parts) -> List[str]:
        return [' '.join(location_parts) + ' New York NY', ''.join(location_parts) + ' New York NY']


    def _parse_geocoding_response_for_borough(self, response) -> Optional[str]:
        if response.get('address_components'):
            boros: List = [comp['long_name'] for comp in response.get(
                'address_components') if comp.get('types') and 'sublocality_level_1' in comp['types']]

            if boros:
                return boros[0]


    def _save_new_geocode(self, db_conn, borough, query_string) -> None:
        db_conn.execute(""" insert into geocodes (lookup_string, borough, geocoding_service) values (%s, %s, %s) """, (query_string, borough, self.GEOCODING_SERVICE_NAME))

