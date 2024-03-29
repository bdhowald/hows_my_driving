from datetime import datetime
from typing import Optional

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Index, Integer, String
from sqlalchemy.dialects.mysql import INTEGER
from sqlalchemy.orm import relationship

from traffic_violations.models.base import Base
from traffic_violations.models.campaign_plate_lookup import CampaignPlateLookup


class PlateLookup(Base):
    """ Represents a record of a submitted plate query """

    __tablename__ = 'plate_lookups'

    # columns
    id = Column(Integer, primary_key=True)
    boot_eligible_under_dvaa_threshold = Column(Boolean, default=False, nullable=False)
    boot_eligible_under_rdaa_threshold = Column(Boolean, default=False, nullable=False)
    bus_lane_camera_violations = Column(INTEGER(unsigned=True), nullable=True)
    count_towards_frequency = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    message_source = Column('lookup_source', String(255), nullable=False)
    message_id = Column(Integer)
    num_tickets = Column(Integer, default=0, nullable=False)
    _observed = Column('observed', String(255), default=None, nullable=True)
    plate = Column(String(16), nullable=False)
    plate_types = Column(String(255))
    red_light_camera_violations = Column(INTEGER(unsigned=True), nullable=True)
    responded_to = Column(Boolean, default=False, nullable=False)
    speed_camera_violations = Column(INTEGER(unsigned=True), nullable=True)
    state = Column(String(8), nullable=False)
    unique_identifier = Column(String(8), nullable=False)
    username = Column('external_username', String(32))

    # associations
    campaigns = relationship("Campaign",
                              secondary=CampaignPlateLookup,
                              back_populates='plate_lookups')

    # indices
    __table_args__ = (
        Index('index_state', 'state'),
        Index('index_plate_state', 'plate', 'state'),
        Index('index_observed', 'observed'),
        Index('index_created_at', 'created_at'),
        Index('index_tweet_id', 'message_id'),
        Index('index_lookup_source', 'lookup_source'),
        Index('index_created_at_tweet_id', 'created_at', 'message_id'),
        Index('index_created_at_lookup_source', 'created_at','lookup_source'),
        Index('index_twitter_handle', 'external_username'),
        Index('index_twitter_handle_plate_state', 'external_username', 'plate', 'state'),
        Index('index_count_towards_frequency', 'count_towards_frequency'),
        Index('index_num_tickets', 'num_tickets'),
        Index('index_boot_eligible_under_rdaa_threshold', 'boot_eligible_under_rdaa_threshold'),
        Index('index_responded_to', 'responded_to'),
        Index('index_unique_identifier', 'unique_identifier')
    )
