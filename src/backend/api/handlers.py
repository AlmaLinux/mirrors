# coding=utf-8
import asyncio
import itertools
import os
import random
import time
from collections import defaultdict
from inspect import signature
from pathlib import Path
from typing import Optional, Union
from urllib.parse import urljoin

import dateparser
from dataclasses import asdict
from sqlalchemy.orm import joinedload

from api.exceptions import UnknownRepoAttribute
from api.mirror_processor import MirrorProcessor
from db.db_engine import FlaskCacheEngine, FlaskCacheEngineRo
from yaml_snippets.utils import (
    get_config,
    get_mirrors_info,
)
from api.redis import (
    _generate_redis_key_for_the_mirrors_list,
    MIRRORS_LIST_EXPIRED_TIME,
    CACHE_EXPIRED_TIME
)
from api.utils import (
    get_geo_data_by_ip,
    get_aws_subnets,
    get_azure_subnets,
    sort_mirrors_by_distance_and_country,
    randomize_mirrors_within_distance,
)
from yaml_snippets.data_models import (
    RepoData,
    MainConfig,
    MirrorData,
)
from db.models import (
    Url,
    mirrors_urls,
    Mirror,
    get_asn_by_ip,
    is_ip_in_any_subnet,
    Subnet,
)
from db.utils import session_scope
from sqlalchemy.sql.expression import or_
from common.sentry import get_logger

logger = get_logger(__name__)
cache = FlaskCacheEngine.get_instance()
cache_ro = FlaskCacheEngineRo.get_instance()


LENGTH_GEO_MIRRORS_LIST = 10
LENGTH_CLOUD_MIRRORS_LIST = 10
SERVICE_CONFIG_PATH = os.path.join(
    os.environ['CONFIG_ROOT'],
    'mirrors/updates/config.yml'
)
SERVICE_CONFIG_JSON_SCHEMA_DIR_PATH = os.path.join(
    os.environ['SOURCE_PATH'],
    'src/backend/yaml_snippets/json_schemas/service_config'
)
MIRROR_CONFIG_JSON_SCHEMA_DIR_PATH = os.path.join(
    os.environ['SOURCE_PATH'],
    'src/backend/yaml_snippets/json_schemas/mirror_config'
)


def _get_nearest_mirrors_by_network_data(
        ip_address: str,
        get_without_private_mirrors: bool,
        get_without_cloud_mirrors: bool,
        get_mirrors_with_full_set_of_isos: bool,
        get_working_mirrors: bool,
        get_expired_mirrors: bool,
        request_protocol = None,
        request_country = None
) -> list[MirrorData]:
    """
    The function returns mirrors which are in the same subnet or have the same
    ASN as a request's IP
    """

    def _is_additional_mirrors_suitable(
            mirror_data: MirrorData,
            main_list_of_mirrors: list[MirrorData]
    ) -> bool:
        """
        An additional mirror is a mirror
        which is fresh (not outdated), not flapping and public, because
        all suitable private mirrors we already found,
        using ASN or subnets data
        """
        return (
                mirror_data.status == 'ok' and
                not mirror_data.private and
                mirror_data.cloud_type in ('', None) and
                mirror_data not in main_list_of_mirrors
        )

    match = get_geo_data_by_ip(ip_address)
    asn = get_asn_by_ip(ip_address)
    suitable_mirrors = []

    mirrors = get_all_mirrors(
        get_working_mirrors=get_working_mirrors,
        get_expired_mirrors=get_expired_mirrors,
        get_without_cloud_mirrors=get_without_cloud_mirrors,
        get_without_private_mirrors=get_without_private_mirrors,
        get_mirrors_with_full_set_of_isos=get_mirrors_with_full_set_of_isos,
        request_protocol=request_protocol,
        request_country=request_country
    )
    for mirror in mirrors:
        if mirror.status != "ok":
            continue
        if (asn is not None and asn in mirror.asn) or is_ip_in_any_subnet(
                ip_address=ip_address,
                subnets_int=mirror.subnets_int,
        ):
            if mirror.monopoly:
                return [mirror]
            else:
                suitable_mirrors.append(mirror)
    if 1 <= len(suitable_mirrors) < LENGTH_CLOUD_MIRRORS_LIST \
            and match is not None:
        continent, country, _, _, latitude, longitude = match
        not_sorted_additional_mirrors = [
            mirror for mirror in mirrors if _is_additional_mirrors_suitable(
                mirror_data=mirror,
                main_list_of_mirrors=suitable_mirrors,
            )
        ]
        sorted_additional_mirrors = sort_mirrors_by_distance_and_country(
            request_geo_data=(latitude, longitude),
            mirrors=not_sorted_additional_mirrors,
            country=country,
        )
        randomized_additional_mirrors = randomize_mirrors_within_distance(
            mirrors=sorted_additional_mirrors,
            country=country,
        )[:LENGTH_CLOUD_MIRRORS_LIST - len(suitable_mirrors)]
        suitable_mirrors.extend(randomized_additional_mirrors)
    return suitable_mirrors


def _get_nearest_mirrors_by_geo_data(
        ip_address: str,
        get_without_private_mirrors: bool,
        get_without_cloud_mirrors: bool,
        get_mirrors_with_full_set_of_isos: bool,
        get_working_mirrors: bool,
        get_expired_mirrors: bool,
        request_protocol = None,
        request_country = None
) -> list[MirrorData]:
    """
    The function returns nearest N mirrors to a client
    Read comments below to get more information
    """
    match = get_geo_data_by_ip(ip_address)
    mirrors = get_all_mirrors(
        get_working_mirrors=get_working_mirrors,
        get_expired_mirrors=get_expired_mirrors,
        get_without_cloud_mirrors=get_without_cloud_mirrors,
        get_without_private_mirrors=get_without_private_mirrors,
        get_mirrors_with_full_set_of_isos=get_mirrors_with_full_set_of_isos,
        request_protocol=request_protocol,
        request_country=request_country
    )
    # We return all mirrors if we can't
    # determine geo data of a request's IP
    if match is None:
        return mirrors
    continent, country, state, city, latitude, longitude = match

    # sort mirrors by distance and randomize those within specified distance
    # to avoid the same mirrors handling the majority of traffic especially
    # within larger cities
    if city or state or country:
        sorted_mirrors = sort_mirrors_by_distance_and_country(
            request_geo_data=(latitude, longitude),
            mirrors=mirrors,
            country=country,
        )
        mirrors = randomize_mirrors_within_distance(
            mirrors=sorted_mirrors,
            country=country,
        )
    # if we don't have city, country or state data for a requesting IP
    # then geoip isn't very accurate anyway so let's give it a random mirror
    # to spread the load. many IPs are missing this data and this prevents
    # all of those requests from disproportionately hitting mirrors near
    # the geographical center of the US
    else:
        random.shuffle(mirrors)

    return mirrors[:LENGTH_GEO_MIRRORS_LIST]


def _get_nearest_mirrors(
        ip_address: Optional[str],
        get_without_private_mirrors: bool,
        get_without_cloud_mirrors: bool,
        get_mirrors_with_full_set_of_isos: bool,
        get_working_mirrors: bool,
        get_expired_mirrors: bool,
        request_protocol: Optional[str] = None,
        request_country: Optional[str] = None
) -> list[MirrorData]:
    """
    Get the nearest mirrors by geo-data or by subnet/ASN
    """
    if ip_address is None:
        return get_all_mirrors(
            get_working_mirrors=get_working_mirrors,
            get_expired_mirrors=get_expired_mirrors,
            get_without_cloud_mirrors=get_without_cloud_mirrors,
            get_without_private_mirrors=get_without_private_mirrors,
            get_mirrors_with_full_set_of_isos=get_mirrors_with_full_set_of_isos,
            request_protocol=request_protocol,
            request_country=request_country
        )
    suitable_mirrors = _get_nearest_mirrors_by_network_data(
        ip_address=ip_address,
        get_working_mirrors=get_working_mirrors,
        get_expired_mirrors=get_expired_mirrors,
        get_without_cloud_mirrors=get_without_cloud_mirrors,
        get_without_private_mirrors=get_without_private_mirrors,
        get_mirrors_with_full_set_of_isos=get_mirrors_with_full_set_of_isos,
        request_protocol=request_protocol,
        request_country=request_country
    )
    if not suitable_mirrors:
        suitable_mirrors = _get_nearest_mirrors_by_geo_data(
            ip_address=ip_address,
            get_working_mirrors=get_working_mirrors,
            get_expired_mirrors=get_expired_mirrors,
            # we get private and cloud mirrors by network data
            get_without_cloud_mirrors=True,
            get_without_private_mirrors=True,
            get_mirrors_with_full_set_of_isos=get_mirrors_with_full_set_of_isos,
            request_protocol=request_protocol,
            request_country=request_country
        )
    return suitable_mirrors


def get_all_mirrors(
        get_working_mirrors: bool = False,
        get_expired_mirrors: bool = False,
        get_without_cloud_mirrors: bool = False,
        get_without_private_mirrors: bool = False,
        get_mirrors_with_full_set_of_isos: bool = False,
        request_protocol: Optional[str] = None,
        request_country: Optional[str] = None
) -> list[MirrorData]:
    """
    Get the list of all mirrors from cache or regenerate one if it's empty
    :param get_working_mirrors: select mirrors which have status 'ok'
    :param get_expired_mirrors: select mirrors which have status 'expired'
    :param get_without_cloud_mirrors: select mirrors without those who are
           hosted in clouds (Azure/AWS)
    :param get_without_private_mirrors: select mirrors without those who are
           hosted behind NAT
    :param get_mirrors_with_full_set_of_isos: select mirrors which have full
           set of ISOs and them artifacts (CHECKSUM, manifests)
           per each version and architecture
    """
    mirrors = get_all_mirrors_db(
        get_working_mirrors=get_working_mirrors,
        get_expired_mirrors=get_expired_mirrors,
        get_without_cloud_mirrors=get_without_cloud_mirrors,
        get_without_private_mirrors=get_without_private_mirrors,
        get_mirrors_with_full_set_of_isos=get_mirrors_with_full_set_of_isos
    )
    random.shuffle(mirrors)
    if request_protocol:
        for mirror in mirrors[:]:
            try:
                mirror.urls[request_protocol]
            except KeyError:
                mirrors.remove(mirror)
    if request_country:
        for mirror in mirrors[:]:
            if mirror.geolocation.country.lower() != request_country.lower():
                mirrors.remove(mirror)
    return mirrors


def get_all_mirrors_db(
        get_working_mirrors: bool = False,
        get_expired_mirrors: bool = False,
        get_without_cloud_mirrors: bool = False,
        get_without_private_mirrors: bool = False,
        get_mirrors_with_full_set_of_isos: bool = False,
        bypass_cache: bool = False
) -> list[MirrorData]:
    """
    Get a mirrors list from DB
    :param get_working_mirrors: select mirrors which have status 'ok'
    :param get_expired_mirrors: select mirrors which have status 'expired'
    :param get_without_cloud_mirrors: select mirrors without those who are
           hosted in clouds (Azure/AWS)
    :param get_without_private_mirrors: select mirrors without those who are
           hosted behind NAT
    :param get_mirrors_with_full_set_of_isos: select mirrors which have full
           set of ISOs and them artifacts (CHECKSUM, manifests)
           per each version and architecture
    """
    mirrors_list = []

    cache_key = _generate_redis_key_for_the_mirrors_list(
        get_working_mirrors=get_working_mirrors,
        get_expired_mirrors=get_expired_mirrors,
        get_without_cloud_mirrors=get_without_cloud_mirrors,
        get_without_private_mirrors=get_without_private_mirrors,
        get_mirrors_with_full_set_of_isos=get_mirrors_with_full_set_of_isos
    )

    if not bypass_cache:
        mirrors = cache_ro.get(cache_key)
        if mirrors:
            return mirrors

    with session_scope() as session:
        mirrors_query = session.query(
            Mirror
        ).options(
            joinedload(Mirror.urls),
            joinedload(Mirror.subnets)
        ).order_by(
            Mirror.continent,
            Mirror.country,
        )
        if get_without_private_mirrors:
            mirrors_query = mirrors_query.filter(
                or_(
                    Mirror.private.is_(False),
                    Mirror.private.is_(None)
                ),
            )
        if get_mirrors_with_full_set_of_isos:
            mirrors_query = mirrors_query.filter(
                Mirror.has_full_iso_set.is_(True),
            )
        or_filter = []
        if get_expired_mirrors:
            or_filter.append(Mirror.status.is_('expired'))
        if get_working_mirrors:
            or_filter.append(Mirror.status.is_('ok'))
        if or_filter:
            mirrors_query = mirrors_query.filter(
                or_(*or_filter)
            )
        if get_without_cloud_mirrors:
            mirrors_query = mirrors_query.filter(
                Mirror.cloud_type.is_(''),
            )
        mirrors = mirrors_query.all()
        for mirror in mirrors:
            mirror_data = mirror.to_dataclass()
            mirrors_list.append(mirror_data)

    cache.set(cache_key, mirrors_list, 86400)
    return mirrors_list


def _is_vault_repo(
        version: str,
        vault_versions: list[str],
        repo: Optional[RepoData],
) -> bool:
    """
    Check that the repo is vault or not.
      The function returns True if repo is vault and
      returns False if the one isn't vault
    :param version: version of requested a mirrors list
    :param vault_versions: the list of global vault versions
    :param repo: repo of requested a mirrors list
    """

    if version in vault_versions or (repo is not None and repo.vault):
        return True
    return False


def get_allowed_arch(
        arch: str,
        arches: list[str],
) -> str:
    if arch not in arches:
        raise UnknownRepoAttribute(
            'Unknown architecture "%s". Allowed list of arches "%s"',
            arch,
            ', '.join(arches),
        )
    return arch


def get_allowed_version(
        versions: list[str],
        vault_versions: list[str],
        duplicated_versions: dict[str, str],
        version: str,
) -> str:

    if version not in versions and version not in vault_versions:
        try:
            major_version = next(
                ver for ver in duplicated_versions if version.startswith(ver)
            )
            return duplicated_versions[major_version]
        except StopIteration:
            raise UnknownRepoAttribute(
                'Unknown version "%s". Allowed list of versions "%s"',
                version,
                ', '.join(versions + vault_versions),
            )
    elif version in versions and version in duplicated_versions:
        return duplicated_versions[version]
    else:
        return version


def get_mirrors_list(
        ip_address: Optional[str],
        version: str,
        arch: Optional[str],
        repository: Optional[str],
        request_protocol: Optional[str] = None,
        request_country: Optional[str] = None,
        iso_list: bool = False,
        debug_info: bool = False,
        redis_key: Optional[str] = None
) -> Union[str, dict]:
    mirrors_list = []
    config = get_config(
        logger=logger,
        path_to_config=SERVICE_CONFIG_PATH,
        path_to_json_schema=SERVICE_CONFIG_JSON_SCHEMA_DIR_PATH,
    )
    versions = config.versions
    duplicated_versions = config.duplicated_versions
    vault_versions = config.vault_versions
    vault_mirror = config.vault_mirror
    repos = {
        repo.name: repo for repo in config.repos
    }  # type: dict[str, RepoData]
    if not iso_list and repository not in repos:
        raise UnknownRepoAttribute(
            'Unknown repository "%s". Allowed list of repositories "%s"',
            repository,
            ', '.join(repos.keys()),
        )
    version = get_allowed_version(
        versions=versions,
        vault_versions=vault_versions,
        duplicated_versions=duplicated_versions,
        version=version,
    )
    if iso_list:
        repo_path = f'isos/{arch}'
        repo = None
    else:
        repo = repos[repository]
        repo_path = repo.path

    # if a client requests global vault version or vault repo
    if _is_vault_repo(
            version=version,
            vault_versions=vault_versions,
            repo=repo
    ):
        return [os.path.join(
            vault_mirror,
            version,
            repo_path,
        )]


    if redis_key:
        nearest_mirrors = cache_ro.get(redis_key)
        from_cache = True
        if not nearest_mirrors:
            from_cache = False
    if not redis_key or not nearest_mirrors:
        nearest_mirrors = _get_nearest_mirrors(
            ip_address=ip_address,
            get_mirrors_with_full_set_of_isos=iso_list,
            get_without_private_mirrors=iso_list,
            get_working_mirrors=True,
            get_without_cloud_mirrors=iso_list,
            get_expired_mirrors=False,
            request_protocol=request_protocol,
            request_country=request_country
        )
    if debug_info:
        data = defaultdict(dict)
        match = get_geo_data_by_ip(ip_address)
        (
            continent,
            country,
            state,
            city_name,
            latitude,
            longitude,
        ) = (None, None, None, None, None, None) if not match else match
        data['geodata'][ip_address] = {
            'continent': continent,
            'country': country,
            'state': state,
            'city': city_name,
            'latitude': latitude,
            'longitude': longitude,
        }
        for mirror in nearest_mirrors:
            data['mirrors'][mirror.name] = asdict(mirror)
        return data
    for mirror in nearest_mirrors:
        if request_protocol:
            full_mirror_path = urljoin(
                mirror.urls[request_protocol] + '/',
                f'{version}/{repo_path}',
                )
        else:
            full_mirror_path = urljoin(
                mirror.mirror_url + '/',
                f'{version}/{repo_path}',
                )
        if arch:
            full_mirror_path = full_mirror_path.replace('$basearch', arch)
        mirrors_list.append(full_mirror_path)
    if not from_cache:
        cache.set(redis_key, nearest_mirrors, CACHE_EXPIRED_TIME)

    return mirrors_list


def get_isos_list_by_countries(
        ip_address: Optional[str],
) -> tuple[dict[str, list[MirrorData]], list[MirrorData]]:
    mirrors_by_countries = defaultdict(list)
    for mirror_info in get_all_mirrors(
            get_without_private_mirrors=True,
            get_mirrors_with_full_set_of_isos=True,
            get_without_cloud_mirrors=True,
            get_working_mirrors=True,
    ):
        mirrors_by_countries[
            mirror_info.geolocation.country
        ].append(mirror_info)
    nearest_mirrors = _get_nearest_mirrors(
        ip_address=ip_address,
        get_without_private_mirrors=True,
        get_mirrors_with_full_set_of_isos=True,
        get_without_cloud_mirrors=True,
        get_working_mirrors=True,
        get_expired_mirrors=False,
    )
    return mirrors_by_countries, nearest_mirrors


def get_main_isos_table(config: MainConfig) -> dict[str, list[str]]:
    result = defaultdict(list)
    for arch in config.arches:
        result[arch] = [
            version for version in config.versions
            if version not in config.duplicated_versions and
               arch in config.versions_arches.get(version, config.arches)
        ]

    return result
