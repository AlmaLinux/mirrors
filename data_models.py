# coding=utf-8
from json import JSONEncoder
from typing import Optional
from dataclasses import (
    dataclass,
    field,
    is_dataclass,
    asdict,
)
import json


class DataClassesJSONEncoder(JSONEncoder):
    """
    Custom JSON encoder for data classes
    """

    def default(self, o):
        if is_dataclass(o):
            return asdict(o)
        return super().default(o)


@dataclass
class LocationData:
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    @staticmethod
    def load_from_json(dct: dict[str, float]):
        return LocationData(
            latitude=dct.get('latitude'),
            longitude=dct.get('longitude'),
        )


@dataclass
class GeoLocationData:
    continent: Optional[str] = None
    country: Optional[str] = None
    state: Optional[str] = None
    city: Optional[str] = None

    @staticmethod
    def load_from_json(dct: dict[str, str]):
        return GeoLocationData(
            continent=dct.get('continent'),
            country=dct.get('country'),
            state=dct.get('state_province'),
            city=dct.get('city'),
        )


@dataclass
class MirrorData:
    status: str = "ok"
    cloud_type: str = ''
    cloud_region: str = ''
    private: bool = False
    location: Optional[LocationData] = None
    geolocation: Optional[GeoLocationData] = None
    name: Optional[str] = None
    update_frequency: Optional[str] = None
    sponsor_name: Optional[str] = None
    sponsor_url: Optional[str] = None
    email: Optional[str] = None
    ip: Optional[str] = None
    ipv6: Optional[bool] = None
    isos_link: Optional[str] = None
    asn: Optional[str] = None
    monopoly: bool = False
    urls: dict[str, str] = field(default_factory=dict)
    subnets: list[str] = field(default_factory=list)

    @staticmethod
    def load_from_json(dct: dict):
        return MirrorData(
            status=dct.get('status'),
            cloud_type=dct.get('cloud_type'),
            cloud_region=dct.get('cloud_region'),
            private=dct.get('private'),
            location=LocationData.load_from_json(
                dct=dct.get('location') or {},
            ),
            geolocation=GeoLocationData.load_from_json(
                dct=dct.get('geolocation') or {},
            ),
            name=dct.get('name'),
            update_frequency=dct.get('update_frequency'),
            sponsor_name=dct.get('sponsor_name'),
            sponsor_url=dct.get('sponsor_url'),
            email=dct.get('email'),
            ip=dct.get('ip'),
            ipv6=dct.get('ipv6'),
            isos_link=dct.get('isos_link'),
            asn=dct.get('asn'),
            urls=dct.get('urls'),
            subnets=dct.get('subnets'),
            monopoly=dct.get('monopoly')
        )

    def to_json(self):
        return json.dumps(self, cls=DataClassesJSONEncoder)


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
