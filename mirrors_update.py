#!/usr/bin/env python3

import logging
import os
import dateparser
import shutil
from collections import defaultdict
from pathlib import Path
from typing import Dict, AnyStr, List, Union

import requests
import yaml

REQUIRED_MIRROR_PROTOCOLS = (
    'https',
    'http',
)
ALL_MIRROR_PROTOCOLS = (
    *REQUIRED_MIRROR_PROTOCOLS,
    'rsync',
)
DEFAULT_ARCH = 'x86_64'

# set User-Agent for python-requests
HEADERS = {
    'User-Agent': 'libdnf (AlmaLinux 8.3; generic; Linux.x86_64)'
}

logging.basicConfig(level=logging.INFO)


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
) -> bool:
    """
    Check mirror availability
    :param mirror_info: the dictionary which contains info about a mirror
                        (name, address, update frequency, sponsor info, email)
    :param versions: the list of versions which should be provided by a mirror
    :param repos: the list of repos which should be provided by a mirror
    """
    logging.info('Checking mirror "%s"...', mirror_info['name'])
    try:
        addresses = mirror_info['address']  # type: Dict[AnyStr, AnyStr]
        mirror_url = next(iter([
            address for protocol_type, address in addresses.items()
            if protocol_type in REQUIRED_MIRROR_PROTOCOLS
        ]))
    except StopIteration:
        logging.error(
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
            except requests.RequestException:
                logging.warning(
                    'Mirror "%s" is not available for version '
                    '"%s" and repo path "%s"',
                    mirror_info['name'],
                    version,
                    repo_path,
                )
                return False
    logging.info(
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
    except requests.RequestException:
        logging.error(
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
                logging.error(
                    'Mirror file "%s" doesn\'t have name of the mirror',
                    config_path,
                )
                continue
            if 'address' not in mirror_info:
                logging.error(
                    'Mirror file "%s" doesn\'t have addresses of the mirror',
                    config_path,
                )
                continue
            if mirror_available(
                mirror_info=mirror_info,
                versions=versions,
                repos=repos,
            ):
                set_repo_status(mirror_info,allowed_outdate)
                result.append(mirror_info)
    return result


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
            logging.warning(
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
        *(
            protocol.upper() for protocol in ALL_MIRROR_PROTOCOLS
        ),
    )

    header_separator = f"| {' | '.join(['---'] * len(columns_names))} |"
    table_header = f"| {' | '.join(columns_names)} |\n{header_separator}"
    address_prefixes = defaultdict(lambda: 'Link')
    address_prefixes.update({
        'https': 'Mirror',
        'http': 'Mirror',
        'rsync': 'Link',
    })
    with open(mirrors_table_path, 'a') as mirrors_table_file:
        logging.info('Generate mirrors table')
        mirrors_table_file.write(f'{table_header}\n')
        for mirror_info in verified_mirrors:
            logging.info(
                'Adding mirror "%s" to mirrors table',
                mirror_info['name']
            )
            addresses = mirror_info['address']
            for protocol in ALL_MIRROR_PROTOCOLS:
                if protocol in addresses:
                    link = f'[{address_prefixes[protocol]}]' \
                           f'({addresses[protocol]})'
                else:
                    link = ''
                mirror_info[f'{protocol}_link'] = link
            table_row = '|'.join((
                mirror_info['name'],
                f"[{mirror_info['sponsor']}]({mirror_info['sponsor_url']})",
                mirror_info['status'],
                *(
                    mirror_info[f'{protocol}_link'] for protocol
                    in ALL_MIRROR_PROTOCOLS
                ),
            ))
            mirrors_table_file.write(f'{table_row}\n')


def main():
    config = get_config()
    versions = config['version']
    repos = config['repos']
    mirrors_table_path = config['mirrors_table']
    shutil.rmtree(
        config['mirrorlist_dir'],
        ignore_errors=True,
    )
    verified_mirrors = get_verified_mirrors(
        mirrors_dir=config['mirrors_dir'],
        versions=versions,
        repos=repos,
        allowed_outdate=config['allowed_outdate']
    )
    if not verified_mirrors:
        logging.error('No available and not expired mirrors found')
        exit(1)
    write_mirrors_to_mirrorslists(
        verified_mirrors=verified_mirrors,
        versions=versions,
        repos=repos,
        mirrorlist_dir=config['mirrorlist_dir'],
    )
    if os.path.exists(mirrors_table_path):
        os.remove(mirrors_table_path)
    generate_mirrors_table(
        mirrors_table_path=mirrors_table_path,
        verified_mirrors=verified_mirrors,
    )


if __name__ == '__main__':
    main()
