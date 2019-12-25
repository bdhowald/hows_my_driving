from sqlalchemy import Column, Index, Integer, String

from traffic_violations.models.base import Base


class Geocode(Base):
    """ Represents a record of a submitted plate query """

    __tablename__ = 'geocodes'

    # columns
    id = Column(Integer, primary_key=True)
    borough = Column(String(255), nullable=False)
    geocoding_service = Column(String(255), nullable=False)
    lookup_string = Column(String(255), nullable=False)

    # indices
    __table_args__ = (
        Index('index_lookup_string', 'lookup_string'),
        Index('index_borough', 'borough'),
        Index('index_geocoding_service', 'geocoding_service')
    )
