from dataclasses import dataclass, field
from typing import List

from traffic_violations.models.response.open_data_service_plate_lookup \
    import OpenDataServicePlateLookup


@dataclass
class TrafficViolationsAggregatorResponse:
    """ Represents the results of a query from open data apis."""
    plate_lookups: List[OpenDataServicePlateLookup] = field(default_factory=list)
