from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Index, Integer, String, Table
from sqlalchemy.orm import relationship

from traffic_violations.models.base import Base

CampaignPlateLookup = Table('campaigns_plate_lookups', Base.metadata,
    Column('plate_lookup_id', Integer, ForeignKey('plate_lookups.id'), primary_key=True),
    Column('campaign_id', Integer, ForeignKey('campaigns.id'), primary_key=True))