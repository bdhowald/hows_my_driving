from sqlalchemy import Column
from sqlalchemy import Index
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.dialects import mysql

from traffic_violations.models.base import Base


class Geocode(Base):
    """ Represents a record of a submitted plate query """

    __tablename__ = 'geocodes'

    # columns
    id = Column(Integer, primary_key=True)
    borough = Column(String(255), nullable=False)
    geocoder_id = Column(mysql.TINYINT, nullable=False)
    lookup_string = Column(String(255), nullable=False)

    # indices
    __table_args__ = (
        Index('index_lookup_string', 'lookup_string'),
        Index('geocodes_geocoder_id_fk_1', 'geocoder_id'),
    )
