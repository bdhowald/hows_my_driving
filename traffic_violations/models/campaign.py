from sqlalchemy import Column, ForeignKey, Index, Integer, String, Table
from sqlalchemy.orm import relationship

from traffic_violations.models.base import Base
from traffic_violations.models.campaign_plate_lookup import CampaignPlateLookup


class Campaign(Base):
    __tablename__ = 'campaigns'

    # columns
    id = Column(Integer, primary_key=True)
    hashtag = Column(String(50), nullable=False)

    # associations
    plate_lookups = relationship("PlateLookup",
                    secondary=CampaignPlateLookup, backref='Campaign')

    # indices
    __table_args__ = (
        Index('index_hashtag', 'hashtag'),
    )