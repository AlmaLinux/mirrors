# coding=utf-8
from dataclasses import (
    dataclass,
    field,
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
from db.db_engine import AsnEngine

logger = get_logger(__name__)

Base = declarative_base()

CACHE_EXPIRED_TIME = 24 * 3600  # 24 hours
MIRROR_CONFIG_SCHEMA = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "type": "object",
    "properties": {
        "cloud_type": {
            "type": "string"
        },
        "cloud_regions": {
            "type": "array",
            "items": {
                "type": "string"
            }
        },
        "name": {
            "type": "string"
        },
        "address": {
            "type": "object",
            "properties": {
                "http": {
                    "type": "string"
                },
                "https": {
                    "type": "string"
                },
                "rsync": {
                    "type": "string"
                },
                "ftp": {
                    "type": "string"
                },
            },
            "anyOf": [
                {
                    "required": [
                        "http",
                    ],
                },
                {
                    "required": [
                        "https",
                    ],
                },
            ],
        },
        "update_frequency": {
            "type": "string"
        },
        "sponsor": {
            "type": "string"
        },
        "sponsor_url": {
            "type": "string"
        },
        "email": {
            "type": "string"
        },
        "asn": {
            "oneOf": [
                {
                    "type": "string",
                },
                {
                    "type": "integer"
                }
            ]
        },
        "subnets": {
            "oneOf": [
                {
                    "type": "array",
                    "items": {
                        "type": "string"
                    }
                },
                {
                    "type": "string",
                }
            ]
        }
    },
    "required": [
        "name",
        "address",
        "update_frequency",
        "sponsor",
        "sponsor_url",
    ],
    "dependencies": {
        "cloud_type": {"required": ["cloud_regions"]}
    }
}
REQUIRED_MIRROR_PROTOCOLS = (
    'https',
    'http',
)
ARCHS = (
    'x86_64',
    'aarch64',
)


class DataClassesJSONEncoder(JSONEncoder):
    """
    Custom JSON encoder for data classes
    """

    def default(self, o):
        if is_dataclass(o):
            return asdict(o)
        return super().default(o)


class DataClassesJSONDecoder(JSONDecoder):
    """
    Custom JSON decoder for data classes
    """

    def __init__(self, *args, **kwargs):
        JSONDecoder.__init__(
            self,
            object_hook=self.object_hook,
            *args,
            **kwargs,
        )

    @staticmethod
    def object_hook(dct):
        logger.info(dct)
        if 'name' in dct:
            return MirrorData(
                name=dct['name'],
                continent=dct['continent'],
                country=dct['country'],
                ip=dct['ip'],
                location=LocationData(
                    latitude=dct['location'].latitude,
                    longitude=dct['location'].longitude,
                ),
                is_expired=dct['is_expired'],
                update_frequency=dct['update_frequency'],
                sponsor_name=dct['sponsor_name'],
                sponsor_url=dct['sponsor_url'],
                email=dct['email'],
                asn=dct['asn'],
                urls=dct['urls'],
                subnets=dct['subnets'],
            )
        if 'latitude' in dct:
            return LocationData(
                latitude=dct['latitude'],
                longitude=dct['longitude'],
            )
        if any(url_type in dct for url_type in REQUIRED_MIRROR_PROTOCOLS):
            return dct


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


@dataclass
class LocationData:
    latitude: float
    longitude: float


@dataclass
class _MirrorYamlDataBase:
    name: AnyStr
    update_frequency: AnyStr
    sponsor_name: AnyStr
    sponsor_url: AnyStr
    email: AnyStr


@dataclass
class _MirrorYamlDataDefaultBase:
    urls: Dict[AnyStr, AnyStr] = field(default_factory=dict)
    subnets: List[AnyStr] = field(default_factory=list)
    asn: Optional[AnyStr] = None
    cloud_type: AnyStr = ''
    cloud_region: AnyStr = ''


@dataclass
class MirrorYamlData(_MirrorYamlDataDefaultBase, _MirrorYamlDataBase):
    pass


@dataclass
class _MirrorDataBase:
    continent: AnyStr
    country: AnyStr
    ip: AnyStr
    location: LocationData


@dataclass
class _MirrorDataDefaultBase:
    isos_link: Optional[AnyStr] = None
    is_expired: Optional[bool] = None


@dataclass
class MirrorData(
    _MirrorDataDefaultBase,
    _MirrorYamlDataDefaultBase,
    _MirrorYamlDataBase,
    _MirrorDataBase,
):
    pass


class Mirror(Base):
    __tablename__ = 'mirrors'

    id = Column(Integer, nullable=False, primary_key=True)
    name = Column(String, nullable=False)
    continent = Column(String, nullable=False)
    country = Column(String, nullable=False)
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
