# coding=utf-8
from __future__ import annotations

import json
from dataclasses import (
    dataclass,
    field,
    is_dataclass,
    asdict,
)
from json import JSONEncoder
from typing import Optional


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
    latitude: float = -90
    longitude: float = -180

    @staticmethod
    def load_from_json(dct: dict[str, float]):
        return LocationData(
            latitude=dct.get('latitude', -91),
            longitude=dct.get('longitude', -181),
        )


@dataclass
class GeoLocationData:

    continent: str = 'Unknown'
    country: str = 'Unknown'
    state_province: str = 'Unknown'
    city: str = 'Unknown'

    def are_mandatory_fields_empty(self) -> bool:
        return any(
            item in ('Unknown', None) for item in (
                self.country,
                self.city,
                self.state_province,
            )
        )

    @staticmethod
    def load_from_json(dct: dict[str, str]):
        return GeoLocationData(
            continent=dct.get('continent', 'Unknown'),
            country=dct.get('country', 'Unknown'),
            state_province=dct.get('state_province', 'Unknown'),
            city=dct.get('city', 'Unknown'),
        )

    def __setattr__(self, key, value):
        if key not in self.__dict__ or self.__dict__[key] in ('Unknown', None):
            self.__dict__[key] = value

    def update_from_existing_object(self, geo_location_data: GeoLocationData):
        self.continent = geo_location_data.continent
        self.country = geo_location_data.country
        self.state_province = geo_location_data.state_province
        self.city = geo_location_data.city


@dataclass
class MirrorData:
    status: str = "Unknown"
    cloud_type: str = ''
    cloud_region: str = ''
    private: bool = False
    mirror_url: Optional[str] = None
    iso_url: Optional[str] = None
    iso_url_kitten: Optional[str] = None
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
    asn: list[str] = None
    monopoly: bool = False
    urls: dict[str, str] = field(default_factory=dict)
    module_urls: dict[str, dict] = field(default_factory=dict)
    has_optional_modules: Optional[str] = None
    subnets: list[str] = field(default_factory=list)
    subnets_int: list[tuple] = field(default_factory=tuple)
    has_full_iso_set: bool = False

    @staticmethod
    def load_from_json(dct: dict):
        return MirrorData(
            status=dct.get('status'),
            cloud_type=dct.get('cloud_type'),
            cloud_region=dct.get('cloud_region'),
            private=dct.get('private'),
            mirror_url=dct.get('mirror_url'),
            iso_url=dct.get('iso_url'),
            iso_url_kitten=dct.get('iso_url_kitten'),
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
            module_urls=dct.get('module_urls'),
            subnets=dct.get('subnets'),
            subnets_int=dct.get('subnets_int'),
            monopoly=dct.get('monopoly'),
            has_full_iso_set=dct.get('has_full_iso_set'),
            has_optional_modules=dct.get('has_optional_modules')
        )

    def to_json(self):
        return json.dumps(self, cls=DataClassesJSONEncoder)


@dataclass
class RepoData:
    name: str
    path: str
    vault: bool
    arches: list = field(default_factory=list)
    versions: list[str] = field(default_factory=list)


@dataclass
class MainConfig:
    allowed_outdate: str
    mirrors_dir: str
    vault_mirror: str
    versions: list[str] = field(default_factory=list)
    optional_module_versions: dict[str, list[str]] = field(
        default_factory=dict
    )
    arches: dict[str, list] = field(default_factory=dict)
    duplicated_versions: dict[str, str] = field(default_factory=dict)
    vault_versions: list[str] = field(default_factory=list)
    required_protocols: list[str] = field(default_factory=list)
    repos: list[RepoData] = field(default_factory=list)
