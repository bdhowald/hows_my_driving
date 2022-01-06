from dataclasses import dataclass, field

from traffic_violations.models.response.open_data_service_plate_lookup \
    import OpenDataServicePlateLookup


@dataclass
class TrafficViolationsAggregatorResponse:
    """ Represents the results of a query from open data apis."""
    plate_lookups: list[OpenDataServicePlateLookup] = field(default_factory=list)
