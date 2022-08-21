import datetime
import ddt
import unittest

from typing import Optional

from traffic_violations.services.apis.queries import covid_19_camera_violations
from unittest.mock import MagicMock

@ddt.ddt
class TestCovid19CameraViolations(unittest.TestCase):

    @ddt.data({
      'start_date': datetime.date(2019, 8, 12),
      'end_date': datetime.date(2022, 3, 26),
      'expected': (
          "https://data.cityofnewyork.us/resource/uvbq-3m68.json?"
          "$select="
          "plate,state,"
          "count(summons_number) AS total_camera_violations,"
          "sum(CASE WHEN violation='PHTO SCHOOL ZN SPEED VIOLATION' THEN 1 ELSE 0 END) AS speed_camera_count,"
          "sum(CASE WHEN violation='FAILURE TO STOP AT RED LIGHT' THEN 1 ELSE 0 END) AS red_light_camera_count&"
          "$where="
          "    violation in('PHTO SCHOOL ZN SPEED VIOLATION', 'FAILURE TO STOP AT RED LIGHT')"
          "and ("
          "issue_date LIKE '08/12/2019' OR "
          "issue_date LIKE '08/13/2019' OR "
          "issue_date LIKE '08/14/2019' OR "
          "issue_date LIKE '08/15/2019' OR "
          "issue_date LIKE '08/16/2019' OR "
          "issue_date LIKE '08/17/2019' OR "
          "issue_date LIKE '08/18/2019' OR "
          "issue_date LIKE '08/19/2019' OR "
          "issue_date LIKE '08/20/2019' OR "
          "issue_date LIKE '08/21/2019' OR "
          "issue_date LIKE '08/22/2019' OR "
          "issue_date LIKE '08/23/2019' OR "
          "issue_date LIKE '08/24/2019' OR "
          "issue_date LIKE '08/25/2019' OR "
          "issue_date LIKE '08/26/2019' OR "
          "issue_date LIKE '08/27/2019' OR "
          "issue_date LIKE '08/28/2019' OR "
          "issue_date LIKE '08/29/2019' OR "
          "issue_date LIKE '08/30/2019' OR "
          "issue_date LIKE '08/31/2019' OR "
          "issue_date LIKE '03/01/2022' OR "
          "issue_date LIKE '03/02/2022' OR "
          "issue_date LIKE '03/03/2022' OR "
          "issue_date LIKE '03/04/2022' OR "
          "issue_date LIKE '03/05/2022' OR "
          "issue_date LIKE '03/06/2022' OR "
          "issue_date LIKE '03/07/2022' OR "
          "issue_date LIKE '03/08/2022' OR "
          "issue_date LIKE '03/09/2022' OR "
          "issue_date LIKE '03/10/2022' OR "
          "issue_date LIKE '03/11/2022' OR "
          "issue_date LIKE '03/12/2022' OR "
          "issue_date LIKE '03/13/2022' OR "
          "issue_date LIKE '03/14/2022' OR "
          "issue_date LIKE '03/15/2022' OR "
          "issue_date LIKE '03/16/2022' OR "
          "issue_date LIKE '03/17/2022' OR "
          "issue_date LIKE '03/18/2022' OR "
          "issue_date LIKE '03/19/2022' OR "
          "issue_date LIKE '03/20/2022' OR "
          "issue_date LIKE '03/21/2022' OR "
          "issue_date LIKE '03/22/2022' OR "
          "issue_date LIKE '03/23/2022' OR "
          "issue_date LIKE '03/24/2022' OR "
          "issue_date LIKE '03/25/2022' OR "
          "issue_date LIKE '03/26/2022' OR "
          "issue_date LIKE '__/__/2020' OR "
          "issue_date LIKE '__/__/2021'"
          ")&"
          "$group=plate,state&"
          "$order=total_camera_violations desc"
      )
    }, {
      'start_date': datetime.date(2019, 8, 12),
      'end_date': datetime.date(2019, 8, 12),
      'expected': (
          "https://data.cityofnewyork.us/resource/uvbq-3m68.json?"
          "$select="
          "plate,state,"
          "count(summons_number) AS total_camera_violations,"
          "sum(CASE WHEN violation='PHTO SCHOOL ZN SPEED VIOLATION' THEN 1 ELSE 0 END) AS speed_camera_count,"
          "sum(CASE WHEN violation='FAILURE TO STOP AT RED LIGHT' THEN 1 ELSE 0 END) AS red_light_camera_count&"
          "$where="
          "    violation in('PHTO SCHOOL ZN SPEED VIOLATION', 'FAILURE TO STOP AT RED LIGHT')"
          "and ("
          "issue_date LIKE '08/12/2019'"
          ")&"
          "$group=plate,state&"
          "$order=total_camera_violations desc"
      )
    }, {
      'start_date': datetime.date(2019, 8, 12),
      'end_date': datetime.date(2019, 8, 28),
      'expected': (
          "https://data.cityofnewyork.us/resource/uvbq-3m68.json?"
          "$select="
          "plate,state,"
          "count(summons_number) AS total_camera_violations,"
          "sum(CASE WHEN violation='PHTO SCHOOL ZN SPEED VIOLATION' THEN 1 ELSE 0 END) AS speed_camera_count,"
          "sum(CASE WHEN violation='FAILURE TO STOP AT RED LIGHT' THEN 1 ELSE 0 END) AS red_light_camera_count&"
          "$where="
          "    violation in('PHTO SCHOOL ZN SPEED VIOLATION', 'FAILURE TO STOP AT RED LIGHT')"
          "and ("
          "issue_date LIKE '08/12/2019' OR "
          "issue_date LIKE '08/13/2019' OR "
          "issue_date LIKE '08/14/2019' OR "
          "issue_date LIKE '08/15/2019' OR "
          "issue_date LIKE '08/16/2019' OR "
          "issue_date LIKE '08/17/2019' OR "
          "issue_date LIKE '08/18/2019' OR "
          "issue_date LIKE '08/19/2019' OR "
          "issue_date LIKE '08/20/2019' OR "
          "issue_date LIKE '08/21/2019' OR "
          "issue_date LIKE '08/22/2019' OR "
          "issue_date LIKE '08/23/2019' OR "
          "issue_date LIKE '08/24/2019' OR "
          "issue_date LIKE '08/25/2019' OR "
          "issue_date LIKE '08/26/2019' OR "
          "issue_date LIKE '08/27/2019' OR "
          "issue_date LIKE '08/28/2019'"
          ")&"
          "$group=plate,state&"
          "$order=total_camera_violations desc"
      )
    }, {
      'start_date': datetime.date(2019, 8, 12),
      'end_date': datetime.date(2019, 8, 12),
      'expected': (
          "https://data.cityofnewyork.us/resource/uvbq-3m68.json?"
          "$select="
          "plate,state,"
          "count(summons_number) AS total_camera_violations,"
          "sum(CASE WHEN violation='PHTO SCHOOL ZN SPEED VIOLATION' THEN 1 ELSE 0 END) AS speed_camera_count,"
          "sum(CASE WHEN violation='FAILURE TO STOP AT RED LIGHT' THEN 1 ELSE 0 END) AS red_light_camera_count&"
          "$where="
          "    violation in('PHTO SCHOOL ZN SPEED VIOLATION', 'FAILURE TO STOP AT RED LIGHT')"
          "and ("
          "issue_date LIKE '08/12/2019'"
          ")&"
          "$group=plate,state&"
          "$order=total_camera_violations desc"
      )
    }, {
      'start_date': datetime.date(2019, 8, 13),
      'end_date': datetime.date(2019, 8, 12),
    }, {
      'start_date': datetime.date(2019, 8, 12),
      'end_date': datetime.date(2019, 9, 7),
      'expected': (
          "https://data.cityofnewyork.us/resource/uvbq-3m68.json?"
          "$select="
          "plate,state,"
          "count(summons_number) AS total_camera_violations,"
          "sum(CASE WHEN violation='PHTO SCHOOL ZN SPEED VIOLATION' THEN 1 ELSE 0 END) AS speed_camera_count,"
          "sum(CASE WHEN violation='FAILURE TO STOP AT RED LIGHT' THEN 1 ELSE 0 END) AS red_light_camera_count&"
          "$where="
          "    violation in('PHTO SCHOOL ZN SPEED VIOLATION', 'FAILURE TO STOP AT RED LIGHT')"
          "and ("
          "issue_date LIKE '08/12/2019' OR "
          "issue_date LIKE '08/13/2019' OR "
          "issue_date LIKE '08/14/2019' OR "
          "issue_date LIKE '08/15/2019' OR "
          "issue_date LIKE '08/16/2019' OR "
          "issue_date LIKE '08/17/2019' OR "
          "issue_date LIKE '08/18/2019' OR "
          "issue_date LIKE '08/19/2019' OR "
          "issue_date LIKE '08/20/2019' OR "
          "issue_date LIKE '08/21/2019' OR "
          "issue_date LIKE '08/22/2019' OR "
          "issue_date LIKE '08/23/2019' OR "
          "issue_date LIKE '08/24/2019' OR "
          "issue_date LIKE '08/25/2019' OR "
          "issue_date LIKE '08/26/2019' OR "
          "issue_date LIKE '08/27/2019' OR "
          "issue_date LIKE '08/28/2019' OR "
          "issue_date LIKE '08/29/2019' OR "
          "issue_date LIKE '08/30/2019' OR "
          "issue_date LIKE '08/31/2019' OR "
          "issue_date LIKE '09/01/2019' OR "
          "issue_date LIKE '09/02/2019' OR "
          "issue_date LIKE '09/03/2019' OR "
          "issue_date LIKE '09/04/2019' OR "
          "issue_date LIKE '09/05/2019' OR "
          "issue_date LIKE '09/06/2019' OR "
          "issue_date LIKE '09/07/2019'"
          ")&"
          "$group=plate,state&"
          "$order=total_camera_violations desc"
      )
    }, {
      'start_date': datetime.date(2019, 8, 12),
      'end_date': datetime.date(2019, 10, 27),
      'expected': (
          "https://data.cityofnewyork.us/resource/uvbq-3m68.json?"
          "$select="
          "plate,state,"
          "count(summons_number) AS total_camera_violations,"
          "sum(CASE WHEN violation='PHTO SCHOOL ZN SPEED VIOLATION' THEN 1 ELSE 0 END) AS speed_camera_count,"
          "sum(CASE WHEN violation='FAILURE TO STOP AT RED LIGHT' THEN 1 ELSE 0 END) AS red_light_camera_count&"
          "$where="
          "    violation in('PHTO SCHOOL ZN SPEED VIOLATION', 'FAILURE TO STOP AT RED LIGHT')"
          "and ("
          "issue_date LIKE '08/12/2019' OR "
          "issue_date LIKE '08/13/2019' OR "
          "issue_date LIKE '08/14/2019' OR "
          "issue_date LIKE '08/15/2019' OR "
          "issue_date LIKE '08/16/2019' OR "
          "issue_date LIKE '08/17/2019' OR "
          "issue_date LIKE '08/18/2019' OR "
          "issue_date LIKE '08/19/2019' OR "
          "issue_date LIKE '08/20/2019' OR "
          "issue_date LIKE '08/21/2019' OR "
          "issue_date LIKE '08/22/2019' OR "
          "issue_date LIKE '08/23/2019' OR "
          "issue_date LIKE '08/24/2019' OR "
          "issue_date LIKE '08/25/2019' OR "
          "issue_date LIKE '08/26/2019' OR "
          "issue_date LIKE '08/27/2019' OR "
          "issue_date LIKE '08/28/2019' OR "
          "issue_date LIKE '08/29/2019' OR "
          "issue_date LIKE '08/30/2019' OR "
          "issue_date LIKE '08/31/2019' OR "
          "issue_date LIKE '10/01/2019' OR "
          "issue_date LIKE '10/02/2019' OR "
          "issue_date LIKE '10/03/2019' OR "
          "issue_date LIKE '10/04/2019' OR "
          "issue_date LIKE '10/05/2019' OR "
          "issue_date LIKE '10/06/2019' OR "
          "issue_date LIKE '10/07/2019' OR "
          "issue_date LIKE '10/08/2019' OR "
          "issue_date LIKE '10/09/2019' OR "
          "issue_date LIKE '10/10/2019' OR "
          "issue_date LIKE '10/11/2019' OR "
          "issue_date LIKE '10/12/2019' OR "
          "issue_date LIKE '10/13/2019' OR "
          "issue_date LIKE '10/14/2019' OR "
          "issue_date LIKE '10/15/2019' OR "
          "issue_date LIKE '10/16/2019' OR "
          "issue_date LIKE '10/17/2019' OR "
          "issue_date LIKE '10/18/2019' OR "
          "issue_date LIKE '10/19/2019' OR "
          "issue_date LIKE '10/20/2019' OR "
          "issue_date LIKE '10/21/2019' OR "
          "issue_date LIKE '10/22/2019' OR "
          "issue_date LIKE '10/23/2019' OR "
          "issue_date LIKE '10/24/2019' OR "
          "issue_date LIKE '10/25/2019' OR "
          "issue_date LIKE '10/26/2019' OR "
          "issue_date LIKE '10/27/2019' OR "
          "issue_date LIKE '09/__/2019'"
          ")&"
          "$group=plate,state&"
          "$order=total_camera_violations desc"
      )
    }, {
      'start_date': datetime.date(2021, 12, 1),
      'end_date': datetime.date(2022, 1, 31),
      'expected': (
          "https://data.cityofnewyork.us/resource/uvbq-3m68.json?"
          "$select="
          "plate,state,"
          "count(summons_number) AS total_camera_violations,"
          "sum(CASE WHEN violation='PHTO SCHOOL ZN SPEED VIOLATION' THEN 1 ELSE 0 END) AS speed_camera_count,"
          "sum(CASE WHEN violation='FAILURE TO STOP AT RED LIGHT' THEN 1 ELSE 0 END) AS red_light_camera_count&"
          "$where="
          "    violation in('PHTO SCHOOL ZN SPEED VIOLATION', 'FAILURE TO STOP AT RED LIGHT')"
          "and ("
          "issue_date LIKE '12/__/2021' OR "
          "issue_date LIKE '1/__/2022'"
          ")&"
          "$group=plate,state&"
          "$order=total_camera_violations desc"
      )
    })
    @ddt.unpack
    def test_get_covid_19_camera_violations_query(
        self,
        start_date: datetime.date,
        end_date: datetime.date,
        expected: Optional[str] = None
    ):
        if start_date > end_date:
            self.assertRaises(ValueError)
        else:
            self.assertEqual(
                covid_19_camera_violations.get_covid_19_camera_violations_query(
                    start_date, end_date
                ),
                expected
            )