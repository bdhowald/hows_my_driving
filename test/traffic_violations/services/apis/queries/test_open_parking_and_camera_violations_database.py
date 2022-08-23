import ddt
import unittest

from typing import List
from typing import Optional

from traffic_violations.models.plate_query import PlateQuery
from traffic_violations.services.apis.queries import open_parking_and_camera_violations_database

@ddt.ddt
class TestFiscalYearDatabase(unittest.TestCase):

    @ddt.data({
        'plate': 'ABC1234',
        'state': 'NY',
        'expected': (
            "https://data.cityofnewyork.us/resource/uvbq-3m68.json?$where=    "
            "plate='ABC1234' AND state='NY'"
        )
    }, {
        'plate': 'ABC1234',
        'plate_types': 'PAS',
        'state': 'NY',
        'expected': (
            "https://data.cityofnewyork.us/resource/uvbq-3m68.json?$where=    "
            "plate='ABC1234' AND state='NY' AND license_type IN ('PAS')"
        )
    }, {
        'plate': 'ABC1234',
        'plate_types': 'COM,PAS',
        'state': 'NY',
        'expected': (
            "https://data.cityofnewyork.us/resource/uvbq-3m68.json?$where=    "
            "plate='ABC1234' AND state='NY' AND license_type IN ('COM','PAS')"
        )
    })
    @ddt.unpack
    def test_get_violations_query(
        self,
        expected: str,
        state: str,
        plate: str,
        plate_types: Optional[str] = None
    ):
        plate_query = _build_plate_query(
            plate=plate,
            state=state,
            plate_types=plate_types
        )
        self.assertEqual(
            open_parking_and_camera_violations_database.get_violations_query(plate_query),
            expected
        )

def _build_plate_query(
    plate: str,
    state: str,
    plate_types: Optional[str] = None
) -> PlateQuery:
    return PlateQuery(
        created_at='2022-08-22 12:34:56',
        message_source='status',
        plate=plate,
        plate_types=plate_types,
        state=state
    )
