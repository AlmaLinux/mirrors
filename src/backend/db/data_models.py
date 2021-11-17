# coding=utf-8
from typing import Optional
from dataclasses import (
    dataclass,
    field,
)
import json


@dataclass
class LocationData:
    latitude: float
    longitude: float


@dataclass
class _MirrorYamlDataBase:
    name: str
    update_frequency: str
    sponsor_name: str
    sponsor_url: str
    email: str
    geolocation: Optional[dict] = field(default_factory=dict)


@dataclass
class _MirrorYamlDataDefaultBase:
    urls: dict[str, str] = field(default_factory=dict)
    subnets: list[str] = field(default_factory=list)
    asn: Optional[str] = None
    cloud_type: str = ''
    cloud_region: str = ''
    private: bool = False


@dataclass
class MirrorYamlData(_MirrorYamlDataDefaultBase, _MirrorYamlDataBase):
    pass


@dataclass
class _MirrorDataBase:
    continent: str
    country: str
    state: str
    city: str
    ip: str
    ipv6: bool
    location: LocationData


@dataclass
class _MirrorDataDefaultBase:
    status: str = "ok"
    isos_link: Optional[str] = None


@dataclass
class MirrorData(
    _MirrorDataDefaultBase,
    _MirrorYamlDataDefaultBase,
    _MirrorYamlDataBase,
    _MirrorDataBase,
):

    @staticmethod
    def load_from_json(dct: dict):
        return MirrorData(
            name=dct['name'],
            continent=dct['continent'],
            country=dct['country'],
            state=dct['state'],
            city=dct['city'],
            ip=dct['ip'],
            ipv6=dct['ipv6'],
            location=LocationData(
                latitude=dct['location']['latitude'],
                longitude=dct['location']['longitude'],
            ),
            status=dct['status'],
            update_frequency=dct['update_frequency'],
            sponsor_name=dct['sponsor_name'],
            sponsor_url=dct['sponsor_url'],
            email=dct['email'],
            asn=dct['asn'],
            urls=dct['urls'],
            subnets=dct['subnets'],
            cloud_type=dct['cloud_type'],
        )

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__)

@dataclass
class RepoData:
    name: str
    path: str
    arches: list[str] = field(default_factory=list)
    versions: list[str] = field(default_factory=list)


@dataclass
class MainConfig:
    allowed_outdate: str
    mirrors_dir: str
    versions: list[str] = field(default_factory=list)
    duplicated_versions: list[str] = field(default_factory=list)
    arches: list[str] = field(default_factory=list)
    required_protocols: list[str] = field(default_factory=list)
    repos: list[RepoData] = field(default_factory=list)
