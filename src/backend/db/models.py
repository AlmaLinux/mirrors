# coding=utf-8
from dataclasses import (
    is_dataclass,
    asdict,
)
from json import (
    JSONEncoder,
    JSONDecoder,
)
from ipaddress import (
    ip_network,
)

from geoip2.errors import AddressNotFoundError
from sqlalchemy import (
    Column,
    String,
    Float,
    Integer,
    Table,
    ForeignKey,
    Boolean,
    DateTime,
    func,
)
from sqlalchemy.ext.declarative import declarative_base
from typing import (
    Dict,
    AnyStr,
    List,
    Optional,
)

from sqlalchemy.orm import relationship
from sqlalchemy.ext.hybrid import hybrid_method

from common.sentry import (
    get_logger,
)
from db.data_models import (
    MirrorData,
    LocationData,
)
from db.db_engine import AsnEngine

logger = get_logger(__name__)

Base = declarative_base()

CACHE_EXPIRED_TIME = 24 * 3600  # 24 hours


class DataClassesJSONEncoder(JSONEncoder):
    """
    Custom JSON encoder for data classes
    """

    def default(self, o):
        if is_dataclass(o):
            return asdict(o)
        return super().default(o)


class Subnet(Base):
    __tablename__ = 'subnets'

    id = Column(Integer, nullable=False, primary_key=True)
    subnet = Column(String, nullable=False)


class Url(Base):
    __tablename__ = 'urls'

    id = Column(Integer, nullable=False, primary_key=True)
    url = Column(String, nullable=False)
    type = Column(String, nullable=False)

    def to_dict(self) -> Dict[AnyStr, AnyStr]:
        return {
            self.type: self.url,
        }


mirrors_urls = Table(
    'mirrors_urls',
    Base.metadata,
    Column(
        'mirror_id', Integer, ForeignKey(
            'mirrors.id',
            ondelete='CASCADE',
        ),
    ),
    Column(
        'url_id', Integer, ForeignKey(
            'urls.id',
            ondelete='CASCADE',
        )
    ),
)

mirrors_subnets = Table(
    'mirrors_subnets',
    Base.metadata,
    Column(
        'mirror_id', Integer, ForeignKey(
            'mirrors.id',
            ondelete='CASCADE',
        ),
    ),
    Column(
        'subnet_id', Integer, ForeignKey(
            'subnets.id',
            ondelete='CASCADE',
        )
    ),
)


class Mirror(Base):
    __tablename__ = 'mirrors'

    id = Column(Integer, nullable=False, primary_key=True)
    name = Column(String, nullable=False)
    continent = Column(String, nullable=False)
    country = Column(String, nullable=False)
    state = Column(String, nullable=True)
    city = Column(String, nullable=True)
    ip = Column(String, nullable=False)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    is_expired = Column(Boolean, nullable=False, default=False)
    update_frequency = Column(DateTime, nullable=False)
    sponsor_name = Column(String, nullable=False)
    sponsor_url = Column(String, nullable=False)
    email = Column(String, nullable=False)
    asn = Column(String, nullable=True)
    cloud_type = Column(String, nullable=True)
    cloud_region = Column(String, nullable=True)
    urls = relationship(
        'Url',
        secondary=mirrors_urls,
        passive_deletes=True,
    )
    subnets = relationship(
        'Subnet',
        secondary=mirrors_subnets,
        passive_deletes=True,
    )

    @hybrid_method
    def conditional_distance(self, lon: float, lat: float):
        """
        Calculate conditional distance between this mirror and some point
        This method is used like instance-method
        """
        return abs(self.longitude - lon) + abs(self.latitude - lat)

    @conditional_distance.expression
    def conditional_distance(self, lon: float, lat: float):
        """
        Calculate conditional distance between this mirror and some point
        This method is used like class-method
        """
        return func.abs(self.longitude - lon) + func.abs(self.latitude - lat)

    def to_dataclass(self) -> MirrorData:
        return MirrorData(
            name=self.name,
            continent=self.continent,
            country=self.country,
            state=self.state,
            city=self.city,
            ip=self.ip,
            location=LocationData(
                latitude=self.latitude,
                longitude=self.longitude,
            ),
            is_expired=self.is_expired,
            update_frequency=self.update_frequency.strftime('%H'),
            sponsor_name=self.sponsor_name,
            sponsor_url=self.sponsor_url,
            email=self.email,
            asn=self.asn,
            urls={
                url.type: url.url for url in self.urls
            },
            subnets=[subnet.subnet for subnet in self.subnets],
            cloud_type=self.cloud_type,
            cloud_region=self.cloud_region,
        )

    def get_subnets(self) -> List[AnyStr]:
        return [subnet.subnet for subnet in self.subnets]


def get_asn_by_ip(
        ip: AnyStr,
) -> Optional[AnyStr]:
    """
    Get ASN by an IP
    """

    db = AsnEngine.get_instance()
    try:
        return str(db.asn(ip).autonomous_system_number)
    except AddressNotFoundError:
        return


def is_ip_in_any_subnet(
        ip_address: AnyStr,
        subnets: List[AnyStr]
) -> bool:
    ip_address = ip_network(ip_address)
    for subnet in subnets:
        subnet = ip_network(subnet)
        if ip_address.version == subnet.version and \
                ip_address.subnet_of(subnet):
            return True
    return False
