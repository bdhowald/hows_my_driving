import os
import requests

from typing import Dict
from typing import List
from typing import Optional

from traffic_violations.models.geocode import Geocode


class LocationService:

    BOROUGH_KEY = 'sublocality_level_1'
    GEOCODING_SERVICE_API_KEY = os.environ['GOOGLE_API_KEY'] if os.environ.get(
            'GOOGLE_API_KEY') else ''
    GEOCODING_SERVICE_ENDPOINT = 'https://maps.googleapis.com/maps/api/geocode/json'
    GEOCODING_SERVICE_NAME = 'google'
    RESULTS_KEY = 'results'
    RESULTS_COMPONENTS_KEY = 'address_components'



    def get_borough_from_location_strings(self, location_parts: List[str]) -> Optional[str]:
        return self._detect_borough(location_parts=location_parts)


    def _detect_borough(self, location_parts) -> Optional[str]:

        # sanitized_parts = [re.sub('\(?[ENSW]/?B\)? *', '', part)
        #                    for part in location_parts]
        location_strs: List[str] = self._normalize_address(
            location_parts=location_parts)

        # location_str = re.sub('[ENSW]B *', '', location_str)
        # lookup_string = ' '.join([location_str, 'New York NY'])

        for geo_string in location_strs:

            # try to find it in the geocodes table first.

            boro_from_geocode: Optional[str] = self._get_existing_geocode(
                query_string=geo_string)

            if boro_from_geocode:
                return boro_from_geocode

            else:
                params: Dict[str, str] = {
                    'address': geo_string,
                    'key': self.GEOCODING_SERVICE_API_KEY}
                results: Optional[Dict[str, str]] = self._make_geocoding_request(
                    params=params)

                if results:
                    boro: str = self._parse_geocoding_response_for_borough(
                        response=results[0])

                    if boro:
                        # insert geocode
                        self._save_new_geocode(
                            borough=boro, query_string=geo_string)

                        # return the boro
                        return boro

        return None


    def _get_existing_geocode(self, query_string) -> Optional[str]:
        geocode: Optional[Geocode] = Geocode.get_by(lookup_string=query_string)

        if geocode:
            return geocode.borough


    def _make_geocoding_request(self, params) -> Optional[Dict[str, str]]:
        req = requests.get(self.GEOCODING_SERVICE_ENDPOINT, params=params)

        if req.json().get(self.RESULTS_KEY):
            return req.json()[self.RESULTS_KEY]
        else:
            return None


    def _normalize_address(self, location_parts) -> List[str]:
        return [f"{' '.join(location_parts)} New York NY",
                f"{' '.join(location_parts)} New York NY"]


    def _parse_geocoding_response_for_borough(self, response) -> Optional[str]:
        if response.get(self.RESULTS_COMPONENTS_KEY):
            boros: List = [comp['long_name'] for comp in response.get(
                self.RESULTS_COMPONENTS_KEY) if comp.get('types') and self.BOROUGH_KEY in comp['types']]

            if boros:
                return boros[0]


    def _save_new_geocode(self, borough, query_string) -> None:
        new_geocode = Geocode(
            borough=borough,
            geocoding_service=self.GEOCODING_SERVICE_NAME,
            lookup_string=query_string)

        Geocode.query.session.add(new_geocode)
        Geocode.query.session.commit()

