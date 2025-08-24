#!/usr/bin/env python3
import asyncio
import json
import os
from asyncio.exceptions import (
    TimeoutError,
    CancelledError,
)
from logging import Logger
from pathlib import Path
from typing import Optional, Union
from urllib.parse import urljoin

import requests
import yaml
from aiohttp import ClientError
from aiohttp.web_exceptions import HTTPError
from aiohttp_retry.types import ClientType
from jsonschema import (
    ValidationError,
    validate,
)

from .data_models import (
    MainConfig,
    RepoData,
    GeoLocationData,
    MirrorData,
    LocationData,
)

# set User-Agent for python-requests
HEADERS = {
    'User-Agent': 'curl/7.61.1',
    'Accept': '*/*',
}
# the list of mirrors which should be always available
WHITELIST_MIRRORS = (
    'repo.almalinux.org',
)

# FIXME: Temporary solution
# https://github.com/AlmaLinux/mirrors/issues/572
WHITELIST_MIRRORS_PER_ARCH_REPO = {
    'eastus.azure.repo.almalinux.org': {
        'arches': [
            'x86_64',
            'aarch64',
        ],
        'repos': [
            'AppStream',
            'BaseOS',
            'HighAvailability',
            'NFV',
            'PowerTools',
            'RT',
            'ResilientStorage',
            'devel',
            'extras',
            'plus',
        ]
    },
    'germanywestcentral.azure.repo.almalinux.org': {
        'arches': [
            'x86_64',
            'aarch64',
        ],
        'repos': [
            'AppStream',
            'BaseOS',
            'HighAvailability',
            'NFV',
            'PowerTools',
            'RT',
            'ResilientStorage',
            'devel',
            'extras',
            'plus',
        ]
    },
    'southeastasia.azure.repo.almalinux.org': {
        'arches': [
            'x86_64',
            'aarch64',
        ],
        'repos': [
            'AppStream',
            'BaseOS',
            'HighAvailability',
            'NFV',
            'PowerTools',
            'RT',
            'ResilientStorage',
            'devel',
            'extras',
            'plus',
        ]
    },
    'westus2.azure.repo.almalinux.org': {
        'arches': [
            'x86_64',
            'aarch64',
        ],
        'repos': [
            'AppStream',
            'BaseOS',
            'HighAvailability',
            'NFV',
            'PowerTools',
            'RT',
            'ResilientStorage',
            'devel',
            'extras',
            'plus',
        ]
    },
}
NUMBER_OF_PROCESSES_FOR_MIRRORS_CHECK = 15
AIOHTTP_TIMEOUT = 30


async def check_tasks(
    created_tasks: list[asyncio.Task],
) -> tuple[bool, Optional[str]]:
    done_tasks, pending_tasks = await asyncio.wait(
        created_tasks,
        return_when=asyncio.FIRST_COMPLETED,
    )
    for future in done_tasks:
        result, reason = future.result()
        if not result:
            for pending_task in pending_tasks:
                pending_task.cancel()
            return False, reason
    if not pending_tasks:
        return True, None
    return await check_tasks(
        pending_tasks,
    )


async def is_url_available(
    url: str,
    http_session: ClientType,
    logger: Logger,
    is_get_request: bool,
    success_msg: Optional[str],
    success_msg_vars: Optional[dict],
    error_msg: Optional[str],
    error_msg_vars: Optional[dict],
    sem: asyncio.Semaphore = None
):
    if not sem:
        sem = asyncio.Semaphore(1)
    async with sem:
        try:
            if is_get_request:
                method = 'get'
            else:
                method = 'head'
            response = await http_session.request(
                method=method,
                url=str(url),
                headers=HEADERS,
            )
            if is_get_request:
                await response.text()
            if success_msg is not None and success_msg_vars is not None:
                logger.info(success_msg, success_msg_vars)
            return True, None
        except (
                TimeoutError,
                HTTPError,
                ClientError,
                # E.g. repomd.xml is broken.
                # It can't be decoded in that case
                UnicodeError,
        ) as err:
            if error_msg is not None and error_msg_vars is not None:
                error_msg_vars['err'] = str(err) or str(type(err))
                logger.warning(error_msg, error_msg_vars)
            return False, str(err) or str(type(err))
        except CancelledError as err:
            return False, str(err) or str(type(err))


def load_json_schema(
    path: str,
) -> dict:
    """
    Load and return JSON schema from a file by path
    """
    with open(path, mode='r') as json_file:
        return json.load(json_file)


def config_validation(
    yaml_data: dict,
    json_schema: dict,
) -> tuple[bool, Optional[str]]:
    """
    Validate some YAML content by JSON schema
    """
    try:
        validate(
            instance=yaml_data,
            schema=json_schema,
        )
        return True, None
    except ValidationError as err:
        return False, err.message


def load_yaml(path: str):
    """
    Read and return content from a YAML file
    """
    with open(path, mode='r') as yaml_file:
        return yaml.safe_load(yaml_file)


def process_main_config(
    yaml_data: dict,
) -> tuple[Optional[MainConfig], Optional[str]]:
    """
    Process data of main config of the mirrors service
    :param yaml_data: YAML data from a file
    of main config of the mirrors service
    """

    def _process_repo_attributes(
            repo_name: str,
            repo_attributes: list[str],
            attributes: dict[str, list[str]],
            version: str = None,
    ) -> list[str]:
        for repo_arch in repo_attributes:
            # Rules for major versions listed
            # in duplicates will be used if found
            if version:
                version = next(
                    (
                        i for i in yaml_data['duplicated_versions']
                        if yaml_data['duplicated_versions'][i] == version
                    ),
                    version,
                )
            ver_attrs = attributes.get(
                version,
                list(set(
                    val for sublist in attributes.values() for val in sublist
                ))
            )
            if (
                repo_arch not in ver_attrs and
                repo_arch not in yaml_data['arches']
            ):
                raise ValidationError(
                    f'Attr "{repo_arch}" of repo "{repo_name}" is absent '
                    f'in the main list of attrs "{", ".join(ver_attrs)}"'
                )
        return repo_attributes

    try:
        vault_versions = [
            str(version) for version in yaml_data.get('vault_versions', [])
        ]
        duplicated_versions = {
            str(major): str(minor) for major, minor
            in yaml_data['duplicated_versions'].items()
        }

        return MainConfig(
            allowed_outdate=yaml_data['allowed_outdate'],
            mirrors_dir=yaml_data['mirrors_dir'],
            vault_mirror=yaml_data.get('vault_mirror'),
            versions=[str(version) for version in yaml_data['versions']],
            optional_module_versions=yaml_data.get(
                'optional_module_versions', {}
            ),
            duplicated_versions=duplicated_versions,
            vault_versions=vault_versions,
            arches=yaml_data['arches'],
            required_protocols=yaml_data['required_protocols'],
            repos=[
                RepoData(
                    name=repo['name'],
                    path=repo['path'],
                    arches=_process_repo_attributes(
                        repo_name=repo['name'],
                        repo_attributes=repo.get('arches', []),
                        attributes=yaml_data['arches'],
                        # Assuming each repo has at least one version
                        version=repo.get('versions', [None])[0]
                    ),
                    versions=_process_repo_attributes(
                        repo_name=repo['name'],
                        repo_attributes=[
                            str(ver) for ver in repo.get('versions', [])
                        ],
                        attributes={
                            str(ver): yaml_data['versions']
                            for ver in repo.get('versions', [])
                        }
                    ),
                    vault=repo.get('vault', False),
                ) for repo in yaml_data['repos']
            ]
        ), None
    except ValidationError as err:
        return None, err.message


def get_config(
    logger: Logger,
    path_to_config: str = os.path.join(
        os.getenv('CONFIG_ROOT', '.'),
        'mirrors/updates/config.yml'
    ),
    path_to_json_schema: str = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        'json_schemas/service_config',
    ),
) -> Optional[MainConfig]:
    """
    Read, validate, parse and return main config of the mirrors service
    """

    config_data = load_yaml(path=path_to_config)
    service_config_version = config_data.get('config_version', 1)
    path_to_json_schema = os.path.join(
        path_to_json_schema,
        f'v{service_config_version}.json',
    )
    json_schema = load_json_schema(path=path_to_json_schema)
    is_valid, err_msg = config_validation(
        yaml_data=config_data,
        json_schema=json_schema,
    )
    if not is_valid:
        logger.error(
            'Main config of the mirror service is invalid because "%s"',
            err_msg,
        )
        return
    config, err_msg = process_main_config(yaml_data=config_data)
    if err_msg:
        logger.error(
            'Main config of the mirror service is invalid because "%s"',
            err_msg,
        )
        return
    return config


def process_mirror_config(
    yaml_data: dict,
    logger: Logger,
    main_config: MainConfig,
) -> MirrorData:
    """
    Process data of a mirror config
    :param yaml_data: YAML data from a file of a mirror config
    :param logger: instance of Logger class
    :param main_config: config of the mirrors service
    """

    def _extract_asn(asn_field: Union[list, int]) -> list:
        if asn_field is None:
            return []
        if isinstance(asn_field, int):
            return [str(asn_field)]
        else:
            return [str(i) for i in asn_field]

    def _get_mirror_subnets(
            subnets_field: Union[list, str],
            mirror_name: str,
    ) -> list:
        if isinstance(subnets_field, str):
            try:
                req = requests.get(subnets_field)
                req.raise_for_status()
                return req.json()
            except (requests.RequestException, json.JSONDecodeError) as err:
                logger.error(
                    'Cannot get subnets of mirror '
                    '"%s" by url "%s" because "%s"',
                    mirror_name,
                    subnets_field,
                    err,
                )
                return []
        return subnets_field
    mirror_info = MirrorData(
        name=yaml_data['name'],
        update_frequency=yaml_data['update_frequency'],
        sponsor_name=yaml_data['sponsor'],
        sponsor_url=yaml_data['sponsor_url'],
        email=yaml_data.get('email', 'unknown'),
        urls={
            _type: url for _type, url in yaml_data['address'].items()
        },
        module_urls={
            module: {
                _type: url for _type, url in urls.items()
            } for module, urls in yaml_data.get('address_optional', {}).items()
        },
        subnets=_get_mirror_subnets(
            subnets_field=yaml_data.get('subnets', []),
            mirror_name=yaml_data['name'],
        ),
        asn=_extract_asn(yaml_data.get('asn')),
        cloud_type=yaml_data.get('cloud_type', ''),
        cloud_region=','.join(yaml_data.get('cloud_regions', [])),
        location=LocationData(),
        geolocation=GeoLocationData.load_from_json(
            yaml_data.get('geolocation', {}),
        ),
        private=yaml_data.get('private', False),
        monopoly=yaml_data.get('monopoly', False),
    )
    mirror_info.mirror_url = get_mirror_url(
        main_config=main_config,
        mirror_info=mirror_info,
    )
    return mirror_info


def get_mirror_config(
    logger: Logger,
    path_to_config: Path,
    main_config: MainConfig,
    path_to_json_schema: str = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        'json_schemas/mirror_config',
    ),
) -> Optional[MirrorData]:
    """
    Read, validate, parse and return config of a mirror
    """
    mirror_data = load_yaml(path=str(path_to_config))
    mirror_config_version = mirror_data.get('config_version', 1)
    path_to_json_schema = os.path.join(
        path_to_json_schema,
        f'v{mirror_config_version}.json',
    )
    json_schema = load_json_schema(path=path_to_json_schema)
    is_valid, err_msg = config_validation(
        yaml_data=mirror_data,
        json_schema=json_schema,
    )
    if not is_valid:
        logger.error(
            'Mirror config "%s" is invalid because "%s"',
            path_to_config.name,
            err_msg,
        )
        return
    config = process_mirror_config(
        yaml_data=mirror_data,
        logger=logger,
        main_config=main_config,
    )
    if err_msg:
        logger.error(
            'Mirror config "%s" is invalid because "%s"',
            path_to_config.name,
            err_msg,
        )
        return
    return config


def get_mirrors_info(
    mirrors_dir: str,
    logger: Logger,
    main_config: MainConfig,
    path_to_json_schema: str = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        'json_schemas/service_config',
    ),
) -> list[MirrorData]:
    """
    Extract info about all mirrors from yaml files
    :param mirrors_dir: path to the directory which contains
           config files of mirrors
    :param logger: instance of Logger class
    :param main_config: main config of the mirrors service
    :param path_to_json_schema: path to JSON schema of a mirror's config
    """
    # global ALL_MIRROR_PROTOCOLS
    result = []
    for config_path in Path(mirrors_dir).rglob('*.yml'):
        mirror_info = get_mirror_config(
            path_to_config=config_path,
            logger=logger,
            path_to_json_schema=path_to_json_schema,
            main_config=main_config,
        )
        if mirror_info is not None:
            result.append(mirror_info)

    return result


def _get_arches_for_version(
    repo_arches: list[str],
    global_arches: list[str],
) -> list[str]:
    """
    Get the available arches for specific version
    :param repo_arches: arches of a specific repo
    :param global_arches: global list of arches
    """

    if repo_arches:
        return repo_arches
    else:
        return global_arches


def _is_permitted_arch_for_this_version_and_repo(
        version: str,
        arch: str,
        arches: dict[str, list[str]]
) -> bool:
    if version not in arches:
        return True
    elif arch in arches[version]:
        return True
    return False


def get_mirror_url(
        main_config: MainConfig,
        mirror_info: MirrorData,
        module: Optional[str] = None
):
    if module:
        return next(
            url for url_type, url in mirror_info.module_urls[module].items()
            if url_type in main_config.required_protocols
        )
    return next(
        url for url_type, url in mirror_info.urls.items()
        if url_type in main_config.required_protocols
    )


def _is_excluded_mirror_by_repo(
    mirror_name: str,
    repo_name: str,
) -> bool:
    return (
        mirror_name in WHITELIST_MIRRORS_PER_ARCH_REPO and
        repo_name not in WHITELIST_MIRRORS_PER_ARCH_REPO[mirror_name]['repos']
    )


def _is_excluded_mirror_by_arch(
    mirror_name: str,
    arch: str,
) -> bool:
    return (
        mirror_name in WHITELIST_MIRRORS_PER_ARCH_REPO and
        arch not in WHITELIST_MIRRORS_PER_ARCH_REPO[mirror_name]['arches']
    )


async def mirror_available(
    mirror_info: MirrorData,
    http_session: ClientType,
    main_config: MainConfig,
    logger: Logger,
) -> tuple[bool, Optional[str]]:
    """
    Check mirror availability
    :param mirror_info: the dictionary which contains info about a mirror
                        (name, address, update frequency, sponsor info, email)
    :param logger: instance of Logger class
    :param main_config: main config of the mirrors service
    :param http_session: async HTTP session
    """
    mirror_name = mirror_info.name
    logger.info('Checking mirror "%s"...', mirror_name)
    if mirror_info.private:
        logger.info(
            'Mirror "%s" is private and won\'t be checked',
            mirror_name,
        )
        return True, None
    urls_for_checking = {}
    for version in main_config.versions:
        # cloud mirrors (Azure/AWS) don't store beta versions
        if mirror_info.cloud_type and 'beta' in version:
            continue
        # don't check duplicated versions
        if version in main_config.duplicated_versions:
            continue
        for repo_data in main_config.repos:
            if _is_excluded_mirror_by_repo(
                mirror_name=mirror_name,
                repo_name=repo_data.name,
            ):
                continue
            if repo_data.vault:
                continue
            base_version = next(
                (
                    i for i in main_config.arches
                    if version.startswith(i)
                ),
                version
            )
            arches = _get_arches_for_version(
                repo_arches=repo_data.arches,
                global_arches=main_config.arches[base_version],
            )
            repo_versions = repo_data.versions
            if repo_versions and version not in repo_versions:
                continue
            for arch in arches:
                if _is_excluded_mirror_by_arch(
                    mirror_name=mirror_name,
                    arch=arch,
                ):
                    continue
                if not _is_permitted_arch_for_this_version_and_repo(
                    version=base_version,
                    arch=arch,
                    arches=main_config.arches,
                ):
                    continue
                repo_path = repo_data.path.replace('$basearch', arch)
                url_for_check = urljoin(
                    urljoin(
                        urljoin(
                            mirror_info.mirror_url + '/',
                            str(version),
                        ) + '/',
                        repo_path,
                    ) + '/',
                    'repodata/repomd.xml',
                )
                urls_for_checking[url_for_check] = {
                    'version': version,
                    'repo_path': repo_path,
                }

    success_msg = (
        'Mirror "%(name)s" is available by url "%(url)s"'
    )
    error_msg = (
        'Mirror "%(name)s" is not available for version '
        '"%(version)s" and repo path "%(repo)s" because "%(err)s"'
    )
    mirror_availability_semaphore = asyncio.Semaphore(5)
    tasks = [asyncio.ensure_future(
        is_url_available(
            url=check_url,
            http_session=http_session,
            logger=logger,
            is_get_request=True,
            success_msg=success_msg,
            success_msg_vars=None,
            error_msg=error_msg,
            error_msg_vars={
                'name': mirror_name,
                'version': url_info['version'],
                'repo': url_info['repo_path'],
            },
            sem=mirror_availability_semaphore
        )
    ) for check_url, url_info in urls_for_checking.items()]
    result, reason = await check_tasks(tasks)

    if result:
        logger.info(
            'Mirror "%s" is available',
            mirror_name,
        )
    return result, reason


async def optional_modules_available(
    mirror_info: MirrorData,
    http_session: ClientType,
    main_config: MainConfig,
    logger: Logger,
    module: str
):
    # this check is not really needed, it's covered it the method that calls this one
    # but just in case
    if not mirror_info.module_urls or not mirror_info.module_urls.get(module):
        return
    
    mirror_name = mirror_info.name
    logger.info(
        'Checking optional module "%s" on mirror "%s"...', module,
        mirror_name,
    )
    if mirror_info.private:
        logger.info(
            'Mirror "%s" is private and optional modules won\'t be checked',
            mirror_name,
        )
        return True

    urls_for_checking = {}
    
    for ver in main_config.optional_module_versions[module]:
        for repo_data in main_config.repos:
            repo_versions = repo_data.versions
            if repo_versions and f'{ver}-{module}' not in repo_versions:
                continue
            if repo_data.vault:
                continue
            arches = _get_arches_for_version(
                repo_arches=repo_data.arches,
                global_arches=main_config.arches[f'{ver}-{module}'],
            )
            for arch in arches:
                if not _is_permitted_arch_for_this_version_and_repo(
                    version=f'{ver}-{module}',
                    arch=arch,
                    arches=main_config.arches,
                ):
                    continue
                repo_path = repo_data.path.replace('$basearch', arch)
                module_urls = mirror_info.module_urls[module]
                url_for_check = urljoin(
                    urljoin(
                        urljoin(
                            (
                                module_urls.get('http') or
                                module_urls.get('https')
                            ) + '/',
                            f'{ver}-{module}',
                        ) + '/',
                        repo_path,
                    ) + '/',
                    'repodata/repomd.xml',
                )
                urls_for_checking[url_for_check] = {
                    'version': f'{ver}-{module}',
                    'repo_path': repo_path,
                    'module': module
                }

    success_msg = (
        'Mirror "%(name)s" optional module "%(module)s" '
        'is available by url "%(url)s"'
    )
    error_msg = (
        'Mirror "%(name)s" optional module "%(module)s" '
        'is not available for version '
        '"%(version)s" and repo path "%(repo)s" because "%(err)s"'
    )
    
    mirror_availability_semaphore = asyncio.Semaphore(5)
    tasks = [asyncio.ensure_future(
        is_url_available(
            url=check_url,
            http_session=http_session,
            logger=logger,
            is_get_request=True,
            success_msg=success_msg,
            success_msg_vars=None,
            error_msg=error_msg,
            error_msg_vars={
                'name': mirror_name,
                'version': url_info['version'],
                'repo': url_info['repo_path'],
                'module': url_info['module'],
            },
            sem=mirror_availability_semaphore
        )
    ) for check_url, url_info in urls_for_checking.items()]
    result, reason = await check_tasks(tasks)

    if result:
        logger.info(
            'Mirror "%s" optional module "%s" is available',
            mirror_name,
            module
        )
        if not mirror_info.has_optional_modules:
            mirror_info.has_optional_modules = module
        else:
            mirror_info.has_optional_modules = (
                f'{mirror_info.has_optional_modules},{module}'
            )
    return result, reason
