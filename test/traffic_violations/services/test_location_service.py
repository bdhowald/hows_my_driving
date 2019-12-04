import logging
import requests
import unittest

from traffic_violations.services.apis.location_service import LocationService
from unittest.mock import MagicMock

class TestLocationService(unittest.TestCase):

    def setUp(self):
        logger = logging.getLogger('hows_my_driving')
        self.location_service = LocationService(logger)

    def test_get_borough_from_location_strings(self):
        bronx_comp = {
            'results': [
                {
                    'address_components': [
                        {
                            'long_name': 'Bronx',
                            'short_name': 'Bronx',
                            'types': [
                                'political',
                                'sublocality',
                                'sublocality_level_1'
                            ]
                        }
                    ]
                }
            ]
        }

        empty_comp = {
            'results': [
                {
                    'address_components': [
                        {}
                    ]
                }
            ]
        }

        req_mock = MagicMock(name='json')
        req_mock.json.return_value = bronx_comp

        get_mock = MagicMock(name='get')
        get_mock.return_value = req_mock

        requests.get = get_mock

        self.assertEqual(self.location_service.get_borough_from_location_strings(['Da', 'Bronx']), 'Bronx')

        req_mock.json.return_value = empty_comp

        self.assertEqual(self.location_service.get_borough_from_location_strings(['no', 'match']), None)
