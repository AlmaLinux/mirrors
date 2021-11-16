#!/usr/bin/env python3
import asyncio
from asyncio.exceptions import TimeoutError
import aiodns
import os
from dataclasses import asdict

import requests
import yaml
import dateparser

from pathlib import Path
from typing import Optional

from aiohttp import ClientSession, ClientError
from sqlalchemy.orm import Session
from jsonschema import (
    ValidationError,
    validate,
)
from api.redis import (
    set_mirror_flapped,
    get_mirror_flapped
)
from api.utils import (
    get_geo_data_by_ip,
    get_coords_by_city
)

from common.sentry import get_logger
from urllib3.exceptions import HTTPError

from db.data_models import MainConfig, RepoData
from db.models import (
    Mirror,
    Url,
    Subnet,
    MirrorData,
    LocationData,
)
from db.data_models import MirrorYamlData
from db.json_schemas import (
    MIRROR_CONFIG_SCHEMA,
    MAIN_CONFIG,
)

# set User-Agent for python-requests
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36',
    "Upgrade-Insecure-Requests": "1",
    "DNT": "1",
    "Accept": "text/html,application/xhtml+xml,"
              "application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate"
}
# the list of mirrors which should be always available
WHITELIST_MIRRORS = (
    'repo.almalinux.org',
)
NUMBER_OF_PROCESSES_FOR_MIRRORS_CHECK = 15
AIOHTTP_TIMEOUT = 30


logger = get_logger(__name__)


def get_config(
        path_to_config: str = os.path.join(
            os.getenv('CONFIG_ROOT'),
            'mirrors/updates/config.yml'
        )
) -> Optional[MainConfig]:
    """
    Read, parse and return mirrorlist config
    """

    with open(path_to_config, mode='r') as config_file:
        config = yaml.safe_load(config_file)
        if 'versions' in config:
            versions = config['versions']
            config['versions'] = [str(version) for version in versions]
        if 'duplicated_versions' in config:
            dup_versions = config['duplicated_versions']
            config['duplicated_versions'] = [str(version) for version
                                             in dup_versions]
        try:
            validate(
                config,
                MAIN_CONFIG,
            )
            repos = []
            for repo in config['repos']:
                for repo_arch in repo.get('arches', []):
                    if repo_arch not in config['arches']:
                        raise ValidationError(
                            message=f'Arch "{repo_arch}" of repo '
                                    f'"{repo["name"]}" is absent '
                                    'in the main list of arches'
                        )
                repos.append(RepoData(
                    name=repo['name'],
                    path=repo['path'],
                    arches=repo.get('arches', []),
                ))
            return MainConfig(
                allowed_outdate=config['allowed_outdate'],
                mirrors_dir=config['mirrors_dir'],
                versions=config['versions'],
                duplicated_versions=config['duplicated_versions'],
                arches=config['arches'],
                required_protocols=config['required_protocols'],
                repos=repos,
            )
        except ValidationError:
            logger.exception('Main config of mirror service is not valid')
            return


def _load_mirror_info_from_yaml_file(
        config_path: Path,
) -> Optional[MirrorYamlData]:
    with open(str(config_path), 'r') as config_file:
        mirror_info = yaml.safe_load(config_file)
        try:
            validate(
                mirror_info,
                MIRROR_CONFIG_SCHEMA,
            )
        except ValidationError:
            logger.exception(
                'Mirror by path "%s" is not valid',
                config_path,
            )
        subnets = mirror_info.get('subnets', [])
        if not isinstance(subnets, list):
            try:
                req = requests.get(subnets)
                req.raise_for_status()
                subnets = req.json()
            except requests.RequestException:
                logger.exception(
                    'Can not get the subnets of mirror "%s" '
                    'by url "%s"',
                    mirror_info['name'],
                    subnets,
                )
                subnets = []
        cloud_regions = mirror_info.get('cloud_regions', [])

        return MirrorYamlData(
            name=mirror_info['name'],
            update_frequency=mirror_info['update_frequency'],
            sponsor_name=mirror_info['sponsor'],
            sponsor_url=mirror_info['sponsor_url'],
            email=mirror_info.get('email', 'unknown'),
            urls={
                _type: url for _type, url in mirror_info['address'].items()
            },
            subnets=subnets,
            asn=mirror_info.get('asn'),
            cloud_type=mirror_info.get('cloud_type', ''),
            cloud_region=','.join(cloud_regions),
            geolocation=mirror_info.get('geolocation', {}),
            private=mirror_info.get('private', False)
        )


def get_mirrors_info(
        mirrors_dir: str,
) -> list[MirrorYamlData]:
    """
    Extract info about all of mirrors from yaml files
    :param mirrors_dir: path to the directory which contains
           config files of mirrors
    """
    # global ALL_MIRROR_PROTOCOLS
    result = []
    for config_path in Path(mirrors_dir).rglob('*.yml'):
        mirror_info = _load_mirror_info_from_yaml_file(
            config_path=config_path,
        )
        result.append(mirror_info)

    return result


async def mirror_available(
        mirror_info: MirrorData,
        versions: list[str],
        repos: list[RepoData],
        http_session: ClientSession,
        arches: list[str],
        required_protocols: list[str],
) -> tuple[str, bool]:
    """
    Check mirror availability
    :param mirror_info: the dictionary which contains info about a mirror
                        (name, address, update frequency, sponsor info, email)
    :param versions: the list of versions which should be provided by a mirror
    :param repos: the list of repos which should be provided by a mirror
    :param arches: list of default arches which are supported by a mirror
    :param http_session: async HTTP session
    :param required_protocols: list of network protocols any of them
                               should be supported by a mirror
    """
    mirror_name = mirror_info.name
    logger.info('Checking mirror "%s"...', mirror_name)
    if mirror_info.private:
        logger.info(
            'Mirror "%s" is private and won\'t be checked',
            mirror_name,
        )
        return mirror_name, True
    try:
        urls = mirror_info.urls  # type: dict[str, str]
        mirror_url = next(
            address for protocol_type, address in urls.items()
            if protocol_type in required_protocols
        )
    except StopIteration:
        logger.error(
            'Mirror "%s" has no one address with protocols "%s"',
            mirror_name,
            required_protocols,
        )
        return mirror_name, False
    for version in versions:
        for repo_data in repos:
            arches = repo_data.arches or arches
            repo_path = repo_data.path.replace('$basearch', arches[0])
            check_url = os.path.join(
                mirror_url,
                str(version),
                repo_path,
                'repodata/repomd.xml',
            )
            try:
                async with http_session.get(
                    check_url,
                    headers=HEADERS,
                    timeout=AIOHTTP_TIMEOUT,
                ) as resp:
                    await resp.text()
                    if resp.status != 200:
                        # if mirror has no valid version/arch combos it is dead
                        logger.error(
                            'Mirror "%s" has one or more invalid repositories',
                            mirror_name
                        )
                        return mirror_name, False
            except (ClientError, TimeoutError) as err:
                # We want to unified error message so I used logging
                # level `error` instead logging level `exception`
                logger.error(
                    'Mirror "%s" is not available for version '
                    '"%s" and repo path "%s" because "%s"',
                    mirror_name,
                    version,
                    repo_path,
                    err,
                )
                return mirror_name, False
    logger.info(
        'Mirror "%s" is available',
        mirror_name,
    )
    return mirror_name, True


async def set_repo_status(
    mirror_info: MirrorData,
    allowed_outdate: str,
    required_protocols: list[str],
    http_session: ClientSession
) -> None:
    """
    Set status of a mirror
    :param mirror_info: info about a mirror
    :param allowed_outdate: allowed mirror lag
    :param required_protocols: list of network protocols any of them
                               should be supported by a mirror
    :param http_session: async http session
    """

    if mirror_info.private:
        mirror_info.status = "ok"
        return
    if await get_mirror_flapped(mirror_name=mirror_info.name):
        mirror_info.status = "flapping"
        return
    urls = mirror_info.urls
    mirror_url = next(
        url for url_type, url in urls.items()
        if url_type in required_protocols
    )
    timestamp_url = os.path.join(
        mirror_url,
        'TIME',
    )
    try:
        async with http_session.get(
            timestamp_url,
            headers=HEADERS,
            timeout=AIOHTTP_TIMEOUT,
            raise_for_status=True
        ) as resp:
            timestamp_response = await resp.text()
    except (asyncio.exceptions.TimeoutError, HTTPError):
        logger.error(
            'Mirror "%s" has no timestamp file by url "%s"',
            mirror_info.name,
            timestamp_url,
        )
        mirror_info.status = "expired"
        return
    try:
        mirror_should_updated_at = dateparser.parse(
            f'now-{allowed_outdate} UTC'
        ).timestamp()
        try:
            mirror_last_updated = float(timestamp_response)
        except ValueError:
            logger.info(
                'Mirror "%s" has broken timestamp file by url "%s"',
                mirror_info.name,
                timestamp_url,
            )
            mirror_info.status = "expired"
            return
        if mirror_last_updated > mirror_should_updated_at:
            mirror_info.status = "ok"
        else:
            mirror_info.status = "expired"
        return
    except AttributeError:
        mirror_info.status = "expired"
        return


async def update_mirror_in_db(
        mirror_info: MirrorYamlData,
        versions: list[str],
        repos: list[RepoData],
        allowed_outdate: str,
        db_session: Session,
        http_session: ClientSession,
        arches: list[str],
        required_protocols: list[str],
        sem: asyncio.Semaphore
) -> None:
    """
    Update record about a mirror in DB in background thread.
    The function remove old record about a mirror and add new record if
        a mirror is actual
    :param mirror_info: extracted info about a mirror from yaml files
    :param versions: the list of versions which should be provided by mirrors
    :param repos: the list of repos which should be provided by mirrors
    :param allowed_outdate: allowed mirror lag
    :param arches: list of default arches which are supported by a mirror
    :param required_protocols: list of network protocols any of them
                               should be supported by a mirror
    :param db_session: session to DB
    :param http_session: async HTTP session
    :param sem: asyncio Semaphore object
    """

    mirror_info = await set_geo_data(mirror_info, sem)
    mirror_name = mirror_info.name
    if mirror_name in WHITELIST_MIRRORS:
        mirror_info.status = "ok"
        is_available = True
    else:
        mirror_name, is_available = await mirror_available(
            mirror_info=mirror_info,
            versions=versions,
            repos=repos,
            http_session=http_session,
            arches=arches,
            required_protocols=required_protocols,
        )
    if not is_available:
        await set_mirror_flapped(mirror_name=mirror_name)
        return
    await set_repo_status(
        mirror_info=mirror_info,
        allowed_outdate=allowed_outdate,
        required_protocols=required_protocols,
        http_session=http_session,
    )
    urls_to_create = [
        Url(
            url=url,
            type=url_type,
        ) for url_type, url in mirror_info.urls.items()
    ]
    for url_to_create in urls_to_create:
        db_session.add(url_to_create)
    mirror_to_create = Mirror(
        name=mirror_info.name,
        continent=mirror_info.continent,
        country=mirror_info.country,
        state=mirror_info.state,
        city=mirror_info.city,
        ip=mirror_info.ip,
        ipv6=mirror_info.ipv6,
        latitude=mirror_info.location.latitude,
        longitude=mirror_info.location.longitude,
        status=mirror_info.status,
        update_frequency=dateparser.parse(
            mirror_info.update_frequency
        ),
        sponsor_name=mirror_info.sponsor_name,
        sponsor_url=mirror_info.sponsor_url,
        email=mirror_info.email,
        cloud_type=mirror_info.cloud_type,
        cloud_region=mirror_info.cloud_region,
        urls=urls_to_create,
        private=mirror_info.private,
    )
    mirror_to_create.asn = mirror_info.asn
    if mirror_info.subnets:
        subnets_to_create = [
            Subnet(
                subnet=subnet,
            ) for subnet in mirror_info.subnets
        ]
        for subnet_to_create in subnets_to_create:
            db_session.add(subnet_to_create)
        mirror_to_create.subnets = subnets_to_create
    logger.debug(
        'Mirror "%s" is created',
        mirror_name,
    )
    db_session.add(mirror_to_create)
    logger.debug(
        'Mirror "%s" is added',
        mirror_name,
    )


async def set_geo_data(
        mirror_info: MirrorYamlData,
        sem: asyncio.Semaphore,
) -> MirrorData:
    """
    Set geo data by IP of a mirror
    :param mirror_info: Dictionary with info about a mirror
    :param sem: asyncio Semaphore
    """
    mirror_name = mirror_info.name
    try:
        resolver = aiodns.DNSResolver(timeout=5, tries=2)
        dns = await resolver.query(mirror_name, 'A')
        ip = dns[0].host
        match = get_geo_data_by_ip(ip)
    except aiodns.error.DNSError:
        logger.exception('Can\'t get IP of mirror %s', mirror_name)
        match = None
        ip = '0.0.0.0'
    try:
        resolver = aiodns.DNSResolver(timeout=5, tries=2)
        dns = await resolver.query(mirror_name, 'AAAA')
        if dns:
            ipv6 = True
        else:
            ipv6 = False
    except aiodns.error.DNSError:
        ipv6 = False
    logger.info('Set geo data for mirror "%s"', mirror_name)
    if match is None:
        state = 'Unknown'
        city = 'Unknown'
        country = 'Unknown'
        continent = 'Unknown'
        ip = ip
        location = LocationData(
            latitude=-91,  # outside range of latitude (-90 to 90)
            longitude=-181,  # outside range of longitude (-180 to 180)
        )
    else:
        continent, country, state, city, latitude, longitude = match
        location = LocationData(
            latitude=latitude,
            longitude=longitude,
        )
    # try to get geo data from yaml
    try:
        country = mirror_info.geolocation.get('country') or country
        state = mirror_info.geolocation.get('state_province') or state or ''
        city = mirror_info.geolocation.get('city') or city or ''
        # we don't need to do lookups except when geolocation is set in yaml
        if mirror_info.geolocation:
            latitude, longitude = await get_coords_by_city(
                city=city, state=state, country=country, sem=sem
            )
            if (0.0, 0.0) != (latitude, longitude):
                location = LocationData(
                    latitude=latitude,
                    longitude=longitude
                )
    except TypeError:
        logger.error(
            'Nominatim likely blocked us'
        )
    return MirrorData(
        continent=continent,
        country=country,
        state=state,
        city=city,
        ip=ip,
        ipv6=ipv6,
        location=location,
        **asdict(mirror_info),
    )
