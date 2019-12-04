import logging
import unittest

from datetime import datetime

from traffic_violations.models.camera_streak_data import CameraStreakData

from traffic_violations.services.apis.open_data_service import OpenDataService

class TestOpenDataService(unittest.TestCase):

    def setUp(self):
        logger = logging.getLogger('hows_my_driving')
        self.open_data_service = OpenDataService(logger)

    def test_find_max_camera_streak(self):
        list_of_camera_times = [
            datetime(2015, 9, 18, 0, 0),
            datetime(2015, 10, 16, 0, 0),
            datetime(2015, 11, 2, 0, 0),
            datetime(2015, 11, 5, 0, 0),
            datetime(2015, 11, 12, 0, 0),
            datetime(2016, 2, 2, 0, 0),
            datetime(2016, 2, 25, 0, 0),
            datetime(2016, 5, 31, 0, 0),
            datetime(2016, 9, 8, 0, 0),
            datetime(2016, 10, 17, 0, 0),
            datetime(2016, 10, 24, 0, 0),
            datetime(2016, 10, 26, 0, 0),
            datetime(2016, 11, 21, 0, 0),
            datetime(2016, 12, 18, 0, 0),
            datetime(2016, 12, 22, 0, 0),
            datetime(2017, 1, 5, 0, 0),
            datetime(2017, 2, 13, 0, 0),
            datetime(2017, 5, 10, 0, 0),
            datetime(2017, 5, 24, 0, 0),
            datetime(2017, 6, 27, 0, 0),
            datetime(2017, 6, 27, 0, 0),
            datetime(2017, 9, 14, 0, 0),
            datetime(2017, 11, 6, 0, 0),
            datetime(2018, 1, 28, 0, 0)
        ]

        result = CameraStreakData(
            min_streak_date='September 8, 2016',
            max_streak=13,
            max_streak_date='June 27, 2017')

        self.assertEqual(self.open_data_service._find_max_camera_violations_streak(
            list_of_camera_times), result)
