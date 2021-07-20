#!/usr/bin/env python3
import os
from dataclasses import asdict

import requests
import yaml
import dateparser
import socket

from pathlib import Path
from typing import (
    Dict,
    AnyStr,
    List,
    Union,
    Tuple,
    Optional,
)

from sqlalchemy.exc import NoResultFound
from uwsgidecorators import thread
from jsonschema import (
    ValidationError,
    validate,
)

from api.utils import get_geo_data_by_ip

from common.sentry import (
    get_logger,
)
from urllib3.exceptions import HTTPError

from db.models import (
    Mirror,
    Url,
    Subnet,
    MirrorData,
    LocationData,
    MirrorYamlData,
    REQUIRED_MIRROR_PROTOCOLS,
    ARCHS,
    MIRROR_CONFIG_SCHEMA,
)
from db.utils import session_scope

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


logger = get_logger(__name__)


def get_config(
        path_to_config: AnyStr = os.path.join(
            os.path.dirname(
                os.path.abspath(__file__),
            ),
            '../../../mirrors/updates/config.yml'
        )
) -> Dict:
    """
    Read, parse and return mirrorlist config
    """

    with open(path_to_config, mode='r') as config_file:
        return yaml.safe_load(config_file)


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
        except ValidationError as err:
            logger.error(
                'Mirror by path "%s" is not valid, because "%s"',
                config_path,
                err,
            )
        subnets = mirror_info.get('subnets', [])
        if not isinstance(subnets, list):
            try:
                req = requests.get(subnets)
                req.raise_for_status()
                subnets = req.json()
            except requests.RequestException as err:
                logger.error(
                    'Can not get the subnets of mirror "%s" '
                    'by url "%s" because "%s"',
                    mirror_info['name'],
                    subnets,
                    err,
                )
                subnets = []
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
            cloud_region=mirror_info.get('cloud_region', ''),
        )


def get_mirrors_info(
        mirrors_dir: AnyStr,
) -> List[MirrorYamlData]:
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


def mirror_available(
        mirror_info: MirrorData,
        versions: List[AnyStr],
        repos: List[Dict[AnyStr, Union[Dict, AnyStr]]],
) -> Tuple[AnyStr, bool]:
    """
    Check mirror availability
    :param mirror_info: the dictionary which contains info about a mirror
                        (name, address, update frequency, sponsor info, email)
    :param versions: the list of versions which should be provided by a mirror
    :param repos: the list of repos which should be provided by a mirror
    """
    mirror_name = mirror_info.name
    logger.info('Checking mirror "%s"...', mirror_name)
    try:
        urls = mirror_info.urls  # type: Dict[AnyStr, AnyStr]
        mirror_url = next(iter([
            address for protocol_type, address in urls.items()
            if protocol_type in REQUIRED_MIRROR_PROTOCOLS
        ]))
    except StopIteration:
        logger.error(
            'Mirror "%s" has no one address with protocols "%s"',
            mirror_name,
            REQUIRED_MIRROR_PROTOCOLS,
        )
        return mirror_name, False
    for version in versions:
        for repo_info in repos:
            repo_path = repo_info['path'].replace('$basearch', ARCHS[0])
            check_url = os.path.join(
                mirror_url,
                str(version),
                repo_path,
                'repodata/repomd.xml',
            )
            try:
                request = requests.get(check_url, headers=HEADERS, timeout=15)
                request.raise_for_status()
            except (requests.RequestException, HTTPError, Exception):
                logger.warning(
                    'Mirror "%s" is not available for version '
                    '"%s" and repo path "%s"',
                    mirror_name,
                    version,
                    repo_path,
                )
                return mirror_name, False
    logger.info(
        'Mirror "%s" is available',
        mirror_name,
    )
    return mirror_name, True


def set_repo_status(
        mirror_info: MirrorData,
        allowed_outdate: AnyStr
) -> None:
    """
    Return status of a mirror
    :param mirror_info: info about a mirror
    :param allowed_outdate: allowed mirror lag
    :return: Status of a mirror: expired or ok
    """

    urls = mirror_info.urls
    mirror_url = next(iter([
        url for url_type, url in urls.items()
        if url_type in REQUIRED_MIRROR_PROTOCOLS
    ]))
    timestamp_url = os.path.join(
        mirror_url,
        'TIME',
    )
    try:
        request = requests.get(
            url=timestamp_url,
            headers=HEADERS,
        )
        request.raise_for_status()
    except (requests.RequestException, HTTPError):
        logger.error(
            'Mirror "%s" has no timestamp file by url "%s"',
            mirror_info.name,
            timestamp_url,
        )
        mirror_info.is_expired = True
        return
    try:
        mirror_should_updated_at = dateparser.parse(
            f'now-{allowed_outdate} UTC'
        ).timestamp()
        try:
            mirror_last_updated = float(request.content)
        except ValueError:
            logger.info(
                'Mirror "%s" has broken timestamp file by url "%s"',
                mirror_info.name,
                timestamp_url,
            )
            mirror_info.is_expired = True
            return
        if mirror_last_updated > mirror_should_updated_at:
            mirror_info.is_expired = False
        else:
            mirror_info.is_expired = True
        return
    except AttributeError:
        mirror_info.is_expired = True
        return


@thread
def update_mirror_in_db(
        mirror_info: MirrorYamlData,
        versions: List[AnyStr],
        repos: List[Dict[AnyStr, Union[Dict, AnyStr]]],
        allowed_outdate: AnyStr,
) -> None:
    """
    Update record about a mirror in DB in background thread.
    The function remove old record about a mirror and add new record if
        a mirror is actual
    :param mirror_info: extracted info about a mirror from yaml files
    :param versions: the list of versions which should be provided by mirrors
    :param repos: the list of repos which should be provided by mirrors
    :param allowed_outdate: allowed mirror lag
    """

    mirror_info = set_geo_data(mirror_info)
    mirror_name = mirror_info.name
    if mirror_name in WHITELIST_MIRRORS:
        mirror_info.is_expired = False
        is_available = True
    else:
        mirror_name, is_available = mirror_available(
            mirror_info=mirror_info,
            versions=versions,
            repos=repos,
        )
    if not is_available:
        return
    set_repo_status(mirror_info, allowed_outdate)
    urls_to_create = [
        Url(
            url=url,
            type=url_type,
        ) for url_type, url in mirror_info.urls.items()
    ]
    with session_scope() as session:
        try:
            session.query(Mirror).filter(
                Mirror.name == mirror_name
            ).delete()
        except NoResultFound:
            pass
        for url_to_create in urls_to_create:
            session.add(url_to_create)
        mirror_to_create = Mirror(
            name=mirror_info.name,
            continent=mirror_info.continent,
            country=mirror_info.country,
            ip=mirror_info.ip,
            latitude=mirror_info.location.latitude,
            longitude=mirror_info.location.longitude,
            is_expired=mirror_info.is_expired,
            update_frequency=dateparser.parse(
                mirror_info.update_frequency
            ),
            sponsor_name=mirror_info.sponsor_name,
            sponsor_url=mirror_info.sponsor_url,
            email=mirror_info.email,
            cloud_type=mirror_info.cloud_type,
            cloud_region=mirror_info.cloud_region,
            urls=urls_to_create,
        )
        mirror_to_create.asn = mirror_info.asn
        if mirror_info.subnets:
            subnets_to_create = [
                Subnet(
                    subnet=subnet,
                ) for subnet in mirror_info.subnets
            ]
            for subnet_to_create in subnets_to_create:
                session.add(subnet_to_create)
        logger.debug(
            'Mirror "%s" is created',
            mirror_name,
        )
        session.add(mirror_to_create)
        session.flush()
        logger.debug(
            'Mirror "%s" is addded',
            mirror_name,
        )


def set_geo_data(
        mirror_info: MirrorYamlData,
) -> MirrorData:
    """
    Set geo data by IP of a mirror
    :param mirror_info: Dict with info about a mirror
    """
    mirror_name = mirror_info.name
    try:
        ip = socket.gethostbyname(mirror_name)
        match = get_geo_data_by_ip(ip)
    except socket.gaierror:
        logger.error('Can\'t get IP of mirror %s', mirror_name)
        match = None
        ip = '0.0.0.0'
    logger.info('Set geo data for mirror "%s"', mirror_name)
    if match is None:
        country = 'Unknown'
        continent = 'Unknown'
        ip = ip
        location = LocationData(
            latitude=-91,  # outside range of latitude (-90 to 90)
            longitude=-181,  # outside range of longitude (-180 to 180)
        )
    else:
        continent, country, latitude, longitude = match
        location = LocationData(
            latitude=latitude,
            longitude=longitude,
        )
    return MirrorData(
        continent=continent,
        country=country,
        ip=ip,
        location=location,
        **asdict(mirror_info),
    )
