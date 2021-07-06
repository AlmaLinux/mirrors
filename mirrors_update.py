#!/usr/bin/env python3

import logging
import os
from copy import copy
from glob import glob

import dateparser
import shutil
import socket
import multiprocessing
from collections import defaultdict
from pathlib import Path
from typing import Dict, AnyStr, List, Union, Tuple
from geoip import IPInfo, open_database
import requests
import yaml
from urllib3.exceptions import HTTPError

REQUIRED_MIRROR_PROTOCOLS = (
    'https',
    'http',
)
ALL_MIRROR_PROTOCOLS = list(REQUIRED_MIRROR_PROTOCOLS)

ARCHS = (
    'x86_64',
    'aarch64',
)

# set User-Agent for python-requests
HEADERS = {
    'User-Agent': 'libdnf (AlmaLinux 8.3; generic; Linux.x86_64)'
}
# the list of mirrors which should be always available
WHITELIST_MIRRORS = (
    'repo.almalinux.org',
)
GEOPIP_DB = 'geoip_db.mmdb'
NUMBER_OF_PROCESSES_FOR_MIRRORS_CHECK = 15


logger = multiprocessing.get_logger()
logger.setLevel(logging.INFO)
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
formatter = logging.Formatter(
    '%(asctime)s | %(name)s |  %(levelname)s: %(message)s'
)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


def get_config(path_to_config: AnyStr = 'config.yml') -> Dict:
    """
    Read, parse and return mirrorlist config
    """

    with open(path_to_config, mode='r') as config_file:
        return yaml.safe_load(config_file)


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
                request = requests.get(check_url, headers=HEADERS, timeout=60)
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
    except requests.RequestException:
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


def get_mirrors_info(
        mirrors_dir: AnyStr,
) -> List[Dict]:
    """
    Extract info about all of mirrors from yaml files
    :param mirrors_dir: path to the directory which contains
           config files of mirrors
    """
    global ALL_MIRROR_PROTOCOLS
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
            ALL_MIRROR_PROTOCOLS.extend(
                protocol for protocol in mirror_info['address'].keys() if
                protocol not in ALL_MIRROR_PROTOCOLS
            )
            result.append(mirror_info)

    return result


def get_verified_mirrors(
        all_mirrors: List[Dict],
        versions: List[AnyStr],
        repos: List[Dict[AnyStr, Union[Dict, AnyStr]]],
        allowed_outdate: AnyStr
) -> List[Dict[AnyStr, Union[Dict, AnyStr]]]:
    """
    Loop through the list of mirrors and return only available
    and not expired mirrors
    :param all_mirrors: extracted info about mirrors from yaml files
    :param versions: the list of versions which should be provided by mirrors
    :param repos: the list of repos which should be provided by mirrors
    :param allowed_outdate: allowed mirror lag
    """

    args = []
    mirrors_info = {}
    for mirror_info in all_mirrors:
        set_mirror_country(mirror_info)
        if mirror_info['name'] in WHITELIST_MIRRORS:
            mirror_info['status'] = 'ok'
            mirrors_info[mirror_info['name']] = mirror_info
            continue
        args.append((mirror_info, versions, repos))
        mirrors_info[mirror_info['name']] = mirror_info
    pool = multiprocessing.Pool(
        processes=NUMBER_OF_PROCESSES_FOR_MIRRORS_CHECK,
    )
    pool_result = pool.map(_helper_mirror_available, args)
    for mirror_name, is_available in pool_result:
        if is_available:
            set_repo_status(mirrors_info[mirror_name], allowed_outdate)
        else:
            del mirrors_info[mirror_name]
    result = sorted(
        mirrors_info.values(),
        key=lambda _mirror_info: _mirror_info['country'],
    )
    return list(result)


def _helper_mirror_available(args):
    return mirror_available(*args)


def write_mirrors_to_mirrorslists(
        verified_mirrors: List[Dict[AnyStr, Union[Dict, AnyStr]]],
        versions: List[AnyStr],
        repos: List[Dict[AnyStr, Union[Dict, AnyStr]]],
        mirrorlist_dir: AnyStr,
) -> None:
    """
    Generate the following folder structure:
        mirrorlist -> <version1> -> <reponame1_mirrorlist>
                                 -> <reponame2_mirrorlist>
                   -> <version2> -> <reponame1_mirrorlist>
    :param verified_mirrors: List of verified and not expired mirrors
    :param versions: the list of versions which should be provided by mirrors
    :param repos: the list of repos which should be provided by mirrors
    :param mirrorlist_dir: the directory which contains mirrorlist files
                           per an each version
    """

    for mirror_info in verified_mirrors:
        if mirror_info['status'] != 'ok':
            logger.warning(
                'Mirror "%s" is expired and isn\'t added to mirrorlist',
                mirror_info['name']
            )
            continue
        addresses = mirror_info['address']
        for version in versions:
            version_dir = os.path.join(
                mirrorlist_dir,
                str(version),
            )
            os.makedirs(version_dir, exist_ok=True)
            for repo_info in repos:
                mirror_url = next(iter([
                    address for protocol_type, address in addresses.items()
                    if protocol_type in REQUIRED_MIRROR_PROTOCOLS
                ]))
                full_mirror_path = os.path.join(
                    mirror_url,
                    str(version),
                    repo_info['path'],
                )
                mirrorlist_path = os.path.join(
                    version_dir,
                    repo_info['name'],
                )
                with open(mirrorlist_path, 'a') as mirrorlist_file:
                    mirrorlist_file.write(f'{full_mirror_path}\n')


def set_mirror_country(
        mirror_info: Dict[AnyStr, Union[Dict, AnyStr]],
) -> None:
    """
    Set country by IP of a mirror
    :param mirror_info: Dict with info about a mirror
    """

    mirror_name = mirror_info['name']
    try:
        ip = socket.gethostbyname(mirror_name)
    except socket.gaierror:
        logger.error('Can\'t get IP of mirror %s', mirror_name)
        mirror_info['country'] = 'Unknown'
        return
    db = open_database(GEOPIP_DB)
    match = db.lookup(ip)  # type: IPInfo
    logger.info('Set country for mirror "%s"', mirror_name)
    if match is None:
        mirror_info['country'] = 'Unknown'
    else:
        country = match.get_info_dict()['country']['names']['en']
        mirror_info['country'] = country


def generate_mirrors_table(
        mirrors_table_path: AnyStr,
        verified_mirrors: List[Dict[AnyStr, Union[Dict, AnyStr]]],
) -> None:
    """
    Generates mirrors table from list verified mirrors
    :param mirrors_table_path: path to file with mirrors table
    :param verified_mirrors: list of verified mirrors
    """
    columns_names = (
        'Name',
        'Sponsor',
        'Status',
        'Location',
        *(
            protocol.upper() for protocol in ALL_MIRROR_PROTOCOLS
        ),
    )

    header_separator = f"| {' | '.join([':---'] * len(columns_names))} |"
    table_header = f"| {' | '.join(columns_names)} |\n{header_separator}"
    address_prefixes = defaultdict(lambda: 'Link')
    address_prefixes.update({
        'https': 'Mirror',
        'http': 'Mirror',
        'rsync': 'Link',
    })
    with open(mirrors_table_path, 'a') as mirrors_table_file:
        logger.info('Generate mirrors table')
        mirrors_table_file.write(f'{table_header}\n')
        for mirror_info in verified_mirrors:
            logger.info(
                'Adding mirror "%s" to mirrors table',
                mirror_info['name']
            )
            addresses = mirror_info['address']
            for protocol in ALL_MIRROR_PROTOCOLS:
                if protocol in addresses:
                    link = f'[{address_prefixes[protocol]}]' \
                           f'({addresses[protocol].strip("/")})'
                else:
                    link = ''
                mirror_info[f'{protocol}_link'] = link
            table_row = '|'.join((
                mirror_info['name'],
                f"[{mirror_info['sponsor']}]({mirror_info['sponsor_url']})",
                mirror_info['status'],
                mirror_info['country'],
                *(
                    mirror_info[f'{protocol}_link'] for protocol
                    in ALL_MIRROR_PROTOCOLS
                ),
            ))
            mirrors_table_file.write(f'{table_row}\n')


def generate_isos_list(
        isos_file: AnyStr,
        isos_dir: AnyStr,
        versions: List[AnyStr],
        verified_mirrors: List[Dict[AnyStr, Union[Dict, AnyStr]]],
) -> None:
    """
    Generates ISOs list from list verified mirrors
    :param isos_file: path to table with archs and versions for ISOs
    :param isos_dir: path to dir with ISOs md files
    :param versions: the list of versions which should be provided by mirrors
    :param verified_mirrors: list of verified mirrors
    """
    mirrors_by_countries = defaultdict(list)
    for mirror_info in verified_mirrors:
        mirrors_by_countries[mirror_info['country']].append(mirror_info)
    with open(isos_file, 'a') as isos_list_file:
        isos_list_file.write(
            '| Architecture | Version |\n'
            '| :--- | :--- |\n'
        )
        for arch in ARCHS:
            os.makedirs(
                os.path.join(
                    isos_dir,
                    arch,
                ),
                exist_ok=True,
            )
            table_row = f'| {arch} | '
            for version in versions:
                table_row = f'{table_row}[{version}](/isos/' \
                            f'{arch}/{version})</br>'
            table_row = f'{table_row} |'
            isos_list_file.write(f'{table_row}\n')
    for arch in ARCHS:
        for version in versions:
            with open(
                    os.path.join(
                        isos_dir,
                        arch,
                        f'{version}.md',
                    ),
                    'a'
            ) as current_isos_file:
                current_isos_file.write(
                    '# AlmaLinux ISOs links  \n'
                    'Here are you can find the list of '
                    'ISOs links for architecture/version'
                    f' `{arch}/{version}` for all of mirrors.  \n'
                    'Also you can use a BitTorrent file for downloading ISOs. '
                    'It should be faster than using '
                    'direct downloading from the mirrors.  \n'
                    'A .torrent file can be found from '
                    'any mirror in ISOs folder.  \n'
                )
                current_isos_file.write(
                    '| Location | Links |\n'
                    '| :--- | :--- |\n'
                )
                for country, country_mirrors in \
                        mirrors_by_countries.items():
                    table_row = f'| {country} | '
                    for mirror_info in country_mirrors:
                        addresses = mirror_info['address']
                        mirror_url = next(iter([
                            address for protocol_type, address in
                            addresses.items()
                            if protocol_type in REQUIRED_MIRROR_PROTOCOLS
                        ]))
                        full_isos_url = os.path.join(
                            mirror_url,
                            str(version),
                            'isos',
                            arch,
                        )
                        table_row = f'{table_row}[{mirror_info["name"]}]' \
                                    f'({full_isos_url})</br>'
                    table_row = f'{table_row} |'
                    current_isos_file.write(f'{table_row}\n')


def clear_old_mirror_content(
        isos_file: AnyStr,
        mirrors_table_path: AnyStr,
        isos_dir: AnyStr,
) -> None:
    """
    Clear old mirror content in *.md files
    """

    if os.path.exists(isos_file):
        os.remove(isos_file)
    if os.path.exists(mirrors_table_path):
        os.remove(mirrors_table_path)
    for file_item in glob(os.path.join(isos_dir, '*')):
        if os.path.isfile(file_item):
            os.remove(file_item)
        elif os.path.isdir(file_item):
            shutil.rmtree(file_item)


def main():
    config = get_config()
    versions = config['versions']
    duplicated_versions = config['duplicated_versions']
    repos = config['repos']
    mirrors_table_path = config['mirrors_table']
    isos_file = 'docs/internal/isos.md'
    isos_dir = 'docs/isos'
    shutil.rmtree(
        config['mirrorlist_dir'],
        ignore_errors=True,
    )
    all_mirrors = get_mirrors_info(
        config['mirrors_dir']
    )
    verified_mirrors = get_verified_mirrors(
        all_mirrors=all_mirrors,
        versions=versions,
        repos=repos,
        allowed_outdate=config['allowed_outdate']
    )
    if not verified_mirrors:
        logger.error('No available and not expired mirrors found')
        exit(1)
    write_mirrors_to_mirrorslists(
        verified_mirrors=verified_mirrors,
        versions=versions,
        repos=repos,
        mirrorlist_dir=config['mirrorlist_dir'],
    )
    clear_old_mirror_content(
        isos_file=isos_file,
        mirrors_table_path=mirrors_table_path,
        isos_dir=isos_dir,
    )
    generate_mirrors_table(
        mirrors_table_path=mirrors_table_path,
        verified_mirrors=verified_mirrors,
    )
    generate_isos_list(
        isos_file=isos_file,
        isos_dir=isos_dir,
        versions=[version for version in versions if version
                  not in duplicated_versions],
        verified_mirrors=verified_mirrors,
    )


if __name__ == '__main__':
    main()
