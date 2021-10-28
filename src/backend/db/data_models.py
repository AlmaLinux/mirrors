# coding=utf-8
from typing import (
    Dict,
    AnyStr,
    List,
    Optional,
)
from dataclasses import (
    dataclass,
    field,
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
    geolocation: Optional[dict]


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
    state: AnyStr
    city: AnyStr
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

    @staticmethod
    def load_from_json(dct: Dict):
        return MirrorData(
            name=dct['name'],
            continent=dct['continent'],
            country=dct['country'],
            state=dct['state'],
            city=dct['city'],
            ip=dct['ip'],
            location=LocationData(
                latitude=dct['location']['latitude'],
                longitude=dct['location']['longitude'],
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


@dataclass
class RepoData:
    name: AnyStr
    path: AnyStr
    arches: List[AnyStr] = field(default_factory=list)


@dataclass
class MainConfig:
    allowed_outdate: AnyStr
    mirrors_dir: AnyStr
    versions: List[AnyStr] = field(default_factory=list)
    duplicated_versions: List[AnyStr] = field(default_factory=list)
    arches: List[AnyStr] = field(default_factory=list)
    required_protocols: List[AnyStr] = field(default_factory=list)
    repos: List[RepoData] = field(default_factory=list)
