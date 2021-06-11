#!/usr/bin/env python3
import multiprocessing
import os

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
)

from sqlalchemy.exc import NoResultFound
from uwsgidecorators import thread

from api.utils import get_geo_data_by_ip

from common.sentry import (
    get_logger,
)
from urllib3.exceptions import HTTPError

from db.models import (
    Mirror,
    Url,
)
from db.utils import session_scope

REQUIRED_MIRROR_PROTOCOLS = (
    'https',
    'http',
)
ARCHS = (
    'x86_64',
)

# set User-Agent for python-requests
HEADERS = {
    'User-Agent': 'libdnf (AlmaLinux 8.3; generic; Linux.x86_64)'
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


def get_mirrors_info(
        mirrors_dir: AnyStr,
) -> List[Dict]:
    """
    Extract info about all of mirrors from yaml files
    :param mirrors_dir: path to the directory which contains
           config files of mirrors
    """
    # global ALL_MIRROR_PROTOCOLS
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
                    mirror_info,
                )
                continue
            # ALL_MIRROR_PROTOCOLS.extend(
            #     protocol for protocol in mirror_info['address'].keys() if
            #     protocol not in ALL_MIRROR_PROTOCOLS
            # )
            result.append(mirror_info)

    return result


def mirror_available(
        mirror_info: Dict[AnyStr, Union[Dict, AnyStr]],
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
    logger.info('Checking mirror "%s"...', mirror_info['name'])
    try:
        logger.info(
            'Checking required protocols of mirror "%s"...',
            mirror_info['name'],
        )
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
        return mirror_info['name'], False
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
                return mirror_info['name'], False
    logger.info(
        'Mirror "%s" is available',
        mirror_info['name']
    )
    return mirror_info['name'], True


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


@thread
def update_mirror_in_db(
        mirror_info: Dict[AnyStr, Union[Dict, AnyStr]],
        versions: List[AnyStr],
        repos: List[Dict[AnyStr, Union[Dict, AnyStr]]],
        allowed_outdate: AnyStr,
) -> None:
    """
    Update record about a mirror in DB in background thread.
    The function remove old record about a mirror and add new record if
        a mirror is actual
    :param all_mirrors: extracted info about mirrors from yaml files
    :param versions: the list of versions which should be provided by mirrors
    :param repos: the list of repos which should be provided by mirrors
    :param allowed_outdate: allowed mirror lag
    """

    set_geo_data(mirror_info)
    mirror_name = mirror_info['name']
    if mirror_name in WHITELIST_MIRRORS:
        mirror_info['status'] = 'ok'
        is_available = True
    else:
        try:
            mirror_name, is_available = mirror_available(
                mirror_info=mirror_info,
                versions=versions,
                repos=repos,
            )
        except Exception as error:
            logger.error(
                'Some unexpected error is occurred '
                'while checking of mirror\'s "%s" availability: "%s"',
                mirror_name,
                error,
            )
    with session_scope() as session:
        try:
            mirrors_for_delete = session.query(Mirror).filter(
                Mirror.name == mirror_name
            ).all()
            for mirror_for_delete in mirrors_for_delete:
                logger.info(
                    'Old mirror "%s" is removed',
                    mirror_for_delete.name,
                )
                session.delete(mirror_for_delete)
        except NoResultFound:
            pass
        if not is_available:
            return
        set_repo_status(mirror_info, allowed_outdate)
        urls_to_create = [
            Url(
                url=url,
                type=url_type,
            ) for url_type, url in mirror_info['address'].items()
        ]
        for url_to_create in urls_to_create:
            session.add(url_to_create)
        mirror_to_create = Mirror(
            name=mirror_info['name'],
            continent=mirror_info['continent'],
            country=mirror_info['country'],
            ip=mirror_info['ip'],
            latitude=mirror_info['location']['lat'],
            longitude=mirror_info['location']['lon'],
            is_expired=mirror_info['status'] == 'expired',
            update_frequency=dateparser.parse(
                mirror_info['update_frequency']
            ),
            sponsor_name=mirror_info['sponsor'],
            sponsor_url=mirror_info['sponsor_url'],
            email=mirror_info.get('email', 'unknown'),
            urls=urls_to_create,
        )
        session.add(mirror_to_create)
        session.flush()


def _helper_mirror_available(args):
    return mirror_available(*args)


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
