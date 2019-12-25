from sqlalchemy import Boolean, Column, Integer, String

from traffic_violations.models.base import Base


class FailedPlateLookup(Base):
    """ Represents a record of a submitted plate query """

    __tablename__ = 'failed_plate_lookups'

    # columns
    id = Column(Integer, primary_key=True)
    message_id = Column(Integer, nullable=False)
    responded_to = Column(Boolean, default=True, nullable=False)
    username = Column('external_username', String(32))