from typing import List
from typing import Optional

from traffic_violations.models.plate_query import PlateQuery
from traffic_violations.services.apis.queries import utils


def get_violations_query(fiscal_year_endpoint: str, plate_query: PlateQuery) -> str:

    where_clause = utils.format_query_string(_build_where_clause(plate_query=plate_query))

    return f"{fiscal_year_endpoint}?{where_clause}"

def _build_where_clause(plate_query: PlateQuery) -> str:
    base_string = (
      f"$where=    plate_id='{plate_query.plate}' "
             f"AND registration_state='{plate_query.state}'"
    )

    if not plate_query.plate_types:
        return base_string

    quoted_plate_types: List[str] = [
        f"'{plate_type}'" for plate_type in plate_query.plate_types.split(',')]

    return (
        f"{base_string} AND plate_type IN ({','.join(quoted_plate_types)})"
    )
