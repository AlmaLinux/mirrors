#!/usr/bin/env python3

from asyncio.exceptions import TimeoutError
import os

import requests
import yaml

from pathlib import Path
from typing import Optional

from aiohttp import ClientSession, ClientError
from jsonschema import (
    ValidationError,
    validate,
)

from data_models import MainConfig, RepoData, GeoLocationData, MirrorData


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
) -> Optional[MirrorData]:
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

        return MirrorData(
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
            geolocation=GeoLocationData.load_from_json(
                mirror_info.get('geolocation', {}),
            ),
            private=mirror_info.get('private', False)
        )


def get_mirrors_info(
        mirrors_dir: str,
) -> list[MirrorData]:
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