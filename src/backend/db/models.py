# coding=utf-8
from collections import defaultdict
from ipaddress import (
    ip_network,
    IPv4Network,
    IPv6Network,
)
from typing import Optional

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
from sqlalchemy.ext.hybrid import hybrid_method
from sqlalchemy.orm import relationship

from common.sentry import get_logger
from db.db_engine import AsnEngine
from yaml_snippets.data_models import (
    MirrorData,
    LocationData,
    GeoLocationData,
)

logger = get_logger(__name__)

Base = declarative_base()


class Subnet(Base):
    __tablename__ = 'subnets'

    id = Column(Integer, nullable=False, primary_key=True)
    subnet = Column(String, nullable=False)
    

class SubnetInt(Base):
    __tablename__ = 'subnets_int'
    
    id = Column(Integer, nullable=False, primary_key=True)
    subnet_start = Column(String, nullable=False)
    subnet_end = Column(String, nullable=False)


class Url(Base):
    __tablename__ = 'urls'

    id = Column(Integer, nullable=False, primary_key=True)
    url = Column(String, nullable=False)
    type = Column(String, nullable=False)

    def to_dict(self) -> dict[str, str]:
        return {
            self.type: self.url,
        }
        
        
class ModuleUrl(Base):
    __tablename__ = 'module_urls'

    id = Column(Integer, nullable=False, primary_key=True)
    url = Column(String, nullable=False)
    type = Column(String, nullable=False)
    module = Column(String, nullable=False)

    def to_dict(self) -> dict[str, str]:
        return {
            self.module: {
                self.type: self.url
            }
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

mirrors_module_urls = Table(
    'mirrors_module_urls',
    Base.metadata,
    Column(
        'mirror_id', Integer, ForeignKey(
            'mirrors.id',
            ondelete='CASCADE',
        ),
    ),
    Column(
        'module_url_id', Integer, ForeignKey(
            'module_urls.id',
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

mirrors_subnets_int = Table(
    'mirrors_subnets_int',
    Base.metadata,
    Column(
        'mirror_id', Integer, ForeignKey(
            'mirrors.id',
            ondelete='CASCADE',
        ),
    ),
    Column(
        'subnet_int_id', Integer, ForeignKey(
            'subnets_int.id',
            ondelete='CASCADE'
        )
    )
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
    status = Column(String, nullable=False)
    update_frequency = Column(DateTime, nullable=False)
    sponsor_name = Column(String, nullable=False)
    mirror_url = Column(String, nullable=False)
    iso_url = Column(String, nullable=False)
    sponsor_url = Column(String, nullable=False)
    email = Column(String, nullable=False)
    asn = Column(String, nullable=True)
    cloud_type = Column(String, nullable=True)
    cloud_region = Column(String, nullable=True)
    private = Column(Boolean, nullable=True, default=False)
    monopoly = Column(Boolean, nullable=True, default=False)
    ipv6 = Column(Boolean, nullable=False, default=False)
    has_full_iso_set = Column(Boolean, nullable=False, default=False)
    has_optional_modules = Column(String, nullable=True)
    urls = relationship(
        'Url',
        secondary=mirrors_urls,
        passive_deletes=True,
    )
    module_urls = relationship(
        'ModuleUrl',
        secondary=mirrors_module_urls,
        passive_deletes=True
    )
    subnets = relationship(
        'Subnet',
        secondary=mirrors_subnets,
        passive_deletes=True,
    )
    subnets_int = relationship(
        'SubnetInt',
        secondary=mirrors_subnets_int,
        passive_deletes=True
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
        def format_module_urls(module_urls):
            result = defaultdict(dict)
            for url in module_urls:
                result[url.module].update({url.type: url.url})
            return dict(result)

        return MirrorData(
            name=self.name,
            ip=self.ip,
            location=LocationData(
                latitude=self.latitude,
                longitude=self.longitude,
            ),
            geolocation=GeoLocationData(
                continent=self.continent,
                country=self.country,
                state_province=self.state,
                city=self.city,
            ),
            status=self.status,
            update_frequency=self.update_frequency.strftime('%H'),
            sponsor_name=self.sponsor_name,
            sponsor_url=self.sponsor_url,
            email=self.email,
            asn=(self.asn or '').split(','),
            urls={
                url.type: url.url for url in self.urls
            },
            module_urls=format_module_urls(self.module_urls),
            subnets=[subnet.subnet for subnet in self.subnets],
            subnets_int=[
                (int(subnet.subnet_start), int(subnet.subnet_end))
                for subnet in self.subnets_int
            ],
            cloud_type=self.cloud_type,
            cloud_region=self.cloud_region,
            private=False if self.private is None else self.private,
            monopoly=False if self.monopoly is None else self.monopoly,
            ipv6=self.ipv6,
            mirror_url=self.mirror_url,
            iso_url=self.iso_url,
            has_full_iso_set=self.has_full_iso_set,
            has_optional_modules=self.has_optional_modules
        )

    def get_subnets(self) -> list[str]:
        return [subnet.subnet for subnet in self.subnets]


def get_asn_by_ip(
        ip: str,
) -> Optional[str]:
    """
    Get ASN by an IP
    """

    db = AsnEngine.get_instance()
    try:
        return str(db.get(ip)['asn']).lstrip('AS')
    except TypeError:
        return


def is_ip_in_any_subnet(
        ip_address: str,
        subnets_int: list[tuple]
) -> bool:
    ip_address = ip_network(ip_address)
    if isinstance(ip_address, IPv4Network) or isinstance(ip_address, IPv6Network):
        ip_int = int(ip_address.network_address)
    for start, end in subnets_int:
        if start <= ip_int <= end:
            return True
    return False
