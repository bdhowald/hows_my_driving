from typing import List
from typing import Optional

from traffic_violations.constants.open_data import endpoints

from traffic_violations.models.plate_query import PlateQuery
from traffic_violations.services.apis.queries import utils


def get_violations_query(plate_query: PlateQuery) -> str:

    where_clause = utils.format_query_string(_build_where_clause(plate_query=plate_query))

    return f"{endpoints.OPEN_PARKING_AND_CAMERA_VIOLATIONS_ENDPOINT}?{where_clause}"

def _build_where_clause(plate_query: PlateQuery) -> str:
    base_string = (
      f"$where=    plate='{plate_query.plate}' "
             f"AND state='{plate_query.state}'"
    )

    if not plate_query.plate_types:
        return base_string

    quoted_plate_types: List[str] = [
        f"'{plate_type}'" for plate_type in plate_query.plate_types]
    
    return (
        f"{base_string} AND license_type IN ({','.join(quoted_plate_types)})"
    )
