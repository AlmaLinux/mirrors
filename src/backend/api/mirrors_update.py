#!/usr/bin/env python3

import os
import dateparser
import socket
from pathlib import Path
from typing import (
    Dict,
    AnyStr,
    List,
    Union,
)
import requests
import yaml

from api.utils import get_geo_data_by_ip

from common.sentry import (
    get_logger,
)
from urllib3.exceptions import HTTPError

REQUIRED_MIRROR_PROTOCOLS = (
    'https',
    'http',
)
DEFAULT_ARCH = 'x86_64'

# set User-Agent for python-requests
HEADERS = {
    'User-Agent': 'libdnf (AlmaLinux 8.3; generic; Linux.x86_64)'
}
# the list of mirrors which should be always available
WHITELIST_MIRRORS = (
    'repo.almalinux.org',
)


logger = get_logger(__name__)


def get_config(
        path_to_config: AnyStr = os.path.join(
            os.path.dirname(
                os.path.abspath(__file__),
            ),
            '../../../mirrors/config.yml'
        )
) -> Dict:
    """
    Read, parse and return mirrorlist config
    """

    with open(path_to_config, mode='r') as config_file:
        return yaml.safe_load(config_file)


def mirror_available(
        mirror_info: Dict[AnyStr, Union[Dict, AnyStr]],
        versions: List[AnyStr],
        repos: List[Dict[AnyStr, Union[Dict, AnyStr]]],
) -> bool:
    """
    Check mirror availability
    :param mirror_info: the dictionary which contains info about a mirror
                        (name, address, update frequency, sponsor info, email)
    :param versions: the list of versions which should be provided by a mirror
    :param repos: the list of repos which should be provided by a mirror
    """
    logger.info('Checking mirror "%s"...', mirror_info['name'])
    try:
        addresses = mirror_info['address']  # type: Dict[AnyStr, AnyStr]
        mirror_url = next(iter([
            address for protocol_type, address in addresses.items()
            if protocol_type in REQUIRED_MIRROR_PROTOCOLS
        ]))
    except StopIteration:
        logger.error(
            'Mirror "%s" has no one address with protocols "%s"',
            mirror_info['name'],
            REQUIRED_MIRROR_PROTOCOLS,
        )
        return False
    for version in versions:
        for repo_info in repos:
            repo_path = repo_info['path'].replace('$basearch', DEFAULT_ARCH)
            check_url = os.path.join(
                mirror_url,
                str(version),
                repo_path,
                'repodata/repomd.xml',
            )
            try:
                request = requests.get(check_url, headers=HEADERS)
                request.raise_for_status()
            except (requests.RequestException, HTTPError):
                logger.warning(
                    'Mirror "%s" is not available for version '
                    '"%s" and repo path "%s"',
                    mirror_info['name'],
                    version,
                    repo_path,
                )
                return False
    logger.info(
        'Mirror "%s" is available',
        mirror_info['name']
    )
    return True


def set_repo_status(
        mirror_info: Dict[AnyStr, Union[Dict, AnyStr]],
        allowed_outdate: AnyStr
) -> None:
    """
    Return status of a mirror
    :param mirror_info: info about a mirror
    :param allowed_outdate: allowed mirror lag
    :return: Status of a mirror: expired or ok
    """

    addresses = mirror_info['address']
    mirror_url = next(iter([
        address for protocol_type, address in addresses.items()
        if protocol_type in REQUIRED_MIRROR_PROTOCOLS
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
            mirror_info['name'],
            timestamp_url,
        )
        mirror_info['status'] = 'expired'
        return
    try:
        mirror_should_updated_at = dateparser.parse(
            f'now-{allowed_outdate} UTC'
        ).timestamp()
        mirror_last_updated = float(request.content)
        if mirror_last_updated > mirror_should_updated_at:
            mirror_info['status'] = 'ok'
        else:
            mirror_info['status'] = 'expired'
        return
    except AttributeError:
        mirror_info['status'] = 'expired'
        return


def get_verified_mirrors(
        mirrors_dir: AnyStr,
        versions: List[AnyStr],
        repos: List[Dict[AnyStr, Union[Dict, AnyStr]]],
        allowed_outdate: AnyStr
) -> List[Dict[AnyStr, Union[Dict, AnyStr]]]:
    """
    Loop through the list of mirrors and return only available
    and not expired mirrors
    :param mirrors_dir: path to the directory which contains
           config files of mirrors
    :param versions: the list of versions which should be provided by mirrors
    :param repos: the list of repos which should be provided by mirrors
    :param allowed_outdate: allowed mirror lag
    """

    result = []
    for config_path in Path(mirrors_dir).rglob('*.yml'):
        with open(str(config_path), 'r') as config_file:
            mirror_info = yaml.safe_load(config_file)
            if 'name' not in mirror_info:
                logger.error(
                    'Mirror file "%s" doesn\'t have name of the mirror',
                    config_path,
                )
                continue
            if 'address' not in mirror_info:
                logger.error(
                    'Mirror file "%s" doesn\'t have addresses of the mirror',
                    config_path,
                )
                continue
            if mirror_info['name'] in WHITELIST_MIRRORS:
                mirror_info['status'] = 'ok'
                set_repo_status(mirror_info, allowed_outdate)
                set_geo_data(mirror_info)
                result.append(mirror_info)
                continue
            if mirror_available(
                    mirror_info=mirror_info,
                    versions=versions,
                    repos=repos,
            ):
                set_repo_status(mirror_info, allowed_outdate)
                set_geo_data(mirror_info)
                result.append(mirror_info)
    return result


def set_geo_data(
        mirror_info: Dict[AnyStr, Union[Dict, AnyStr]],
) -> None:
    """
    Set geo data by IP of a mirror
    :param mirror_info: Dict with info about a mirror
    """

    mirror_name = mirror_info['name']
    try:
        ip = socket.gethostbyname(mirror_name)
        match = get_geo_data_by_ip(ip)
    except socket.gaierror:
        logger.error('Can\'t get IP of mirror %s', mirror_name)
        match = None
        ip = '0.0.0.0'
    logger.info('Set geo data for mirror "%s"', mirror_name)
    if match is None:
        mirror_info['country'] = 'Unknown'
        mirror_info['continent'] = 'Unknown'
        mirror_info['ip'] = ip
        mirror_info['location'] = {
            'lat': -91,  # outside range of latitude (-90 to 90)
            'lon': -181,  # outside range of longitude (-180 to 180)
        }
    else:
        continent, country, latitude, longitude = match
        mirror_info['country'] = country
        mirror_info['continent'] = continent
        mirror_info['ip'] = ip
        mirror_info['location'] = {
            'lat': latitude,
            'lon': longitude,
        }
