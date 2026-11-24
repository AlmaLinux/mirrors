#!/usr/bin/env python3
import asyncio
import json
import os
import time
import xml.etree.ElementTree as ElementTree
import zlib
from asyncio.exceptions import (
    TimeoutError,
    CancelledError,
)
from logging import Logger
from pathlib import Path
from typing import Iterable, NamedTuple, Optional, Union
from urllib.parse import urljoin, urlsplit
from xml.parsers import expat

import requests
import yaml
from aiohttp import ClientError, ClientResponse, ClientResponseError
from aiohttp.web_exceptions import HTTPError
from aiohttp_retry.types import ClientType
from jsonschema import (
    ValidationError,
    validate,
)

from .data_models import (
    DEFAULT_REPOMD_CHECK_TYPES,
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

REPOMD_PATH = 'repodata/repomd.xml'
# DNF (librepo) never sends Accept-Encoding, so the repodata checks don't
# either. That's how they see a mirror that compresses responses anyway.
SKIP_ACCEPT_ENCODING = ('Accept-Encoding',)
# A real repomd.xml is a few KB
MAX_REPOMD_SIZE = 1024 * 1024
# A real revision is a Unix timestamp. Longer ones are ignored.
MAX_REVISION_LENGTH = 64
XML_BASE_ATTRIBUTE = '{http://www.w3.org/XML/1998/namespace}base'
# Statuses which mean a file referenced by repomd.xml isn't there for clients.
# Anything else (timeouts, rate limits, server errors) only gets logged:
# availability is what the repomd.xml request itself checks.
MISSING_FILE_STATUSES = (403, 404, 410)
# Requests in flight to one mirror while its repos are checked
REPO_CHECK_CONCURRENCY = 5
# Seconds to wait before checking a repo with missing files a second time,
# in case the mirror was caught in the middle of a sync
REPODATA_RECHECK_DELAY = 30
# Every file repomd.xml lists, beyond repomd_check_types, is checked for one
# in FULL_CHECK_ROTATION repos per run. Every file of every repo is thus
# checked every FULL_CHECK_ROTATION runs (2 hours), without making each run
# four times slower.
FULL_CHECK_ROTATION = 8
# Seconds between update runs
FULL_CHECK_PERIOD = 15 * 60


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
            # Wait for cancellations to land so callers (e.g. MirrorProcessor.__aexit__)
            # don't close the ClientSession out from under a still-live task.
            if pending_tasks:
                await asyncio.gather(*pending_tasks, return_exceptions=True)
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


class RepodataError(Exception):
    """
    A mirror doesn't serve a repo's metadata in a way DNF can use
    """


class RepomdFile(NamedTuple):
    data_type: str
    href: str
    size: Optional[int]
    # xml:base of the file's location, if it's hosted somewhere else
    base: Optional[str]


class Repomd(NamedTuple):
    revision: Optional[str]
    files: list[RepomdFile]


def _local_name(tag) -> str:
    # comments and processing instructions don't have a str tag
    if not isinstance(tag, str):
        return ''
    return tag.rsplit('}', 1)[-1]


def _find_child(element: ElementTree.Element, name: str):
    return next(
        (child for child in element if _local_name(child.tag) == name),
        None,
    )


def _refuse_dtd(*args):
    raise ValueError('it has a document type declaration')


def parse_repomd(content: bytes) -> Repomd:
    """
    Parse repomd.xml and return its revision and the files it lists
    :param content: raw content of repomd.xml
    :raises ValueError: the content isn't a repomd.xml DNF could use
    """
    start = content.lstrip()[:64].lower()
    if start.startswith(b'<!doctype html') or start.startswith(b'<html'):
        raise ValueError('it is an HTML page')
    try:
        # repomd.xml never has a DTD. Refusing one in the parser, whatever the
        # document's encoding, rules out entity expansion.
        dtd_check = expat.ParserCreate()
        dtd_check.StartDoctypeDeclHandler = _refuse_dtd
        dtd_check.Parse(content, True)
        root = ElementTree.fromstring(content)
    # LookupError: the XML declaration names an unknown encoding
    except (
        expat.ExpatError,
        ElementTree.ParseError,
        LookupError,
        UnicodeError,
    ) as err:
        raise ValueError(str(err)) from err
    if _local_name(root.tag) != 'repomd':
        raise ValueError(
            f'its root element is <{_local_name(root.tag)}>, not <repomd>'
        )
    revision = None
    files = []
    for element in root:
        name = _local_name(element.tag)
        if name == 'revision':
            revision = (element.text or '').strip()
            if not 0 < len(revision) <= MAX_REVISION_LENGTH:
                revision = None
        elif name == 'data':
            location = _find_child(element, 'location')
            if location is None or not location.get('href'):
                continue
            size_element = _find_child(element, 'size')
            try:
                size = int(size_element.text)
            except (AttributeError, TypeError, ValueError):
                size = None
            files.append(RepomdFile(
                data_type=element.get('type', ''),
                href=location.get('href'),
                size=size,
                base=location.get(XML_BASE_ATTRIBUTE),
            ))
    if not any(file.data_type == 'primary' for file in files):
        raise ValueError('it lists no primary metadata')
    return Repomd(revision=revision, files=files)


def _content_encoding(response: ClientResponse) -> str:
    """
    Return the response's content codings other than identity, e.g. "gzip",
    or '' if it isn't encoded
    """
    codings = (
        coding.strip().lower()
        for header in response.headers.getall('Content-Encoding', [])
        for coding in header.split(',')
    )
    return ', '.join(
        coding for coding in codings if coding and coding != 'identity'
    )


async def _read_limited(response: ClientResponse, limit: int) -> bytes:
    """
    Read a response body which shouldn't be larger than limit bytes
    :raises ValueError: the body is larger
    """
    error = f'it is larger than {limit} bytes'
    if response.content_length is not None and response.content_length > limit:
        raise ValueError(error)
    content = bytearray()
    async for chunk in response.content.iter_chunked(64 * 1024):
        content += chunk
        if len(content) > limit:
            raise ValueError(error)
    return bytes(content)


async def fetch_repomd(
    repomd_url: str,
    http_session: ClientType,
    sem: Optional[asyncio.Semaphore] = None,
) -> Repomd:
    """
    Download and parse repomd.xml the way DNF does: without Accept-Encoding
    :raises RepodataError: the mirror doesn't serve a repomd.xml DNF could use
    """
    if sem is None:
        sem = asyncio.Semaphore(1)
    async with sem:
        try:
            async with http_session.request(
                method='get',
                url=repomd_url,
                headers=HEADERS,
                skip_auto_headers=SKIP_ACCEPT_ENCODING,
                allow_redirects=True,
            ) as response:
                response.raise_for_status()
                encoding = _content_encoding(response)
                if encoding:
                    raise RepodataError(
                        f'repomd.xml is sent with Content-Encoding '
                        f'"{encoding}" to clients that did not ask for it, '
                        f"url='{repomd_url}'"
                    )
                content = await _read_limited(response, MAX_REPOMD_SIZE)
        except (TimeoutError, ClientError, HTTPError) as err:
            raise RepodataError(str(err) or str(type(err))) from err
        except ValueError as err:
            raise RepodataError(
                f"repomd.xml is not valid ({err}), url='{repomd_url}'"
            ) from err
    try:
        return parse_repomd(content)
    except ValueError as err:
        raise RepodataError(
            f"repomd.xml is not valid ({err}), url='{repomd_url}'"
        ) from err


def _stop_file_checks(
    file_checks: dict,
    logger: Logger,
    file_url: str,
    reason: str,
):
    if file_checks.get('stopped'):
        return
    file_checks['stopped'] = True
    logger.warning(
        'Stopped checking files listed by repomd.xml on this mirror for this '
        'run, because HEAD "%s" failed: "%s"',
        file_url,
        reason,
    )


async def check_repomd_files(
    repo_url: str,
    repomd: Repomd,
    check_types: Iterable[str],
    http_session: ClientType,
    sem: asyncio.Semaphore,
    logger: Logger,
    file_checks: Optional[dict] = None,
) -> Optional[str]:
    """
    Check that the files repomd.xml lists for check_types exist and have
    the size repomd.xml gives them
    :param repo_url: URL of the repo's directory, ending with "/"
    :param check_types: metadata types to check, "*" for all of them
    :param file_checks: state shared by the repos of one mirror. Once a HEAD
                        gets no usable answer (timeout, rate limit, server
                        error, ...), the files of the other repos aren't
                        checked, so such a mirror costs one failed request
                        instead of one per repo.
    :return: why a file is missing or wrong, or None if all of them are fine
             or couldn't be checked
    """
    if file_checks is None:
        file_checks = {}
    check_types = set(check_types)
    for file in repomd.files:
        if '*' not in check_types and file.data_type not in check_types:
            continue
        try:
            is_elsewhere = bool(
                file.base or
                urlsplit(file.href).scheme or
                file.href.startswith('/')
            )
            file_url = urljoin(repo_url, file.href)
        except ValueError as err:
            logger.warning(
                'Cannot check file "%s" of "%s" because "%s"',
                file.href,
                repo_url,
                err,
            )
            continue
        if is_elsewhere:
            # The file isn't in the repo's directory
            continue
        async with sem:
            if file_checks.get('stopped'):
                return None
            try:
                async with http_session.request(
                    method='head',
                    url=file_url,
                    headers=HEADERS,
                    skip_auto_headers=SKIP_ACCEPT_ENCODING,
                    allow_redirects=True,
                ) as response:
                    status = response.status
                    size = response.content_length
                    encoding = _content_encoding(response)
            except ClientResponseError as err:
                status, size, encoding = err.status, None, ''
            except (TimeoutError, ClientError, HTTPError) as err:
                _stop_file_checks(
                    file_checks=file_checks,
                    logger=logger,
                    file_url=file_url,
                    reason=str(err) or str(type(err)),
                )
                return None
        if status in MISSING_FILE_STATUSES:
            return (
                f"{file.data_type} metadata returns {status}, url='{file_url}'"
            )
        if not 200 <= status < 300:
            _stop_file_checks(
                file_checks=file_checks,
                logger=logger,
                file_url=file_url,
                reason=f'it returns {status}',
            )
            return None
        if (
            status == 200 and
            size is not None and
            file.size is not None and
            size != file.size
        ):
            sent_as = f' with Content-Encoding "{encoding}"' if encoding else ''
            return (
                f'{file.data_type} metadata is {size} bytes{sent_as}, '
                f"repomd.xml says {file.size}, url='{file_url}'"
            )
    return None


async def check_repo_metadata(
    repomd_url: str,
    http_session: ClientType,
    logger: Logger,
    sem: asyncio.Semaphore,
    check_types: Iterable[str],
    error_msg: str,
    error_msg_vars: dict,
    revisions: Optional[dict] = None,
    revision_key: Optional[str] = None,
    recheck_delay: float = REPODATA_RECHECK_DELAY,
    file_checks: Optional[dict] = None,
) -> tuple[bool, Optional[str]]:
    """
    Check a repo the way DNF uses it: repomd.xml must be sent without
    unrequested Content-Encoding and parse, and the files it lists for
    check_types must exist with the size it gives them.
    A repo with missing files is checked again after recheck_delay seconds,
    in case the mirror was caught in the middle of a sync.
    :param repomd_url: URL of the repo's repodata/repomd.xml
    :param revisions: if given, gets the revision of repomd.xml under
                      revision_key when the repo passes
    :param file_checks: state shared by the repos of one mirror,
                        see check_repomd_files
    :return: whether the repo passed, and the reason if it didn't
    """
    try:
        return await _check_repo_metadata(
            repomd_url=repomd_url,
            http_session=http_session,
            logger=logger,
            sem=sem,
            check_types=check_types,
            error_msg=error_msg,
            error_msg_vars=error_msg_vars,
            revisions=revisions,
            revision_key=revision_key,
            recheck_delay=recheck_delay,
            file_checks={} if file_checks is None else file_checks,
        )
    except Exception:
        # A bug here mustn't take mirrors out of the lists, or stop the check
        # of the other mirrors. Pass the repo, as the plain availability check
        # this replaced would have done.
        logger.exception('Cannot check repo "%s"', repomd_url)
        return True, None


async def _check_repo_metadata(
    repomd_url: str,
    http_session: ClientType,
    logger: Logger,
    sem: asyncio.Semaphore,
    check_types: Iterable[str],
    error_msg: str,
    error_msg_vars: dict,
    revisions: Optional[dict],
    revision_key: Optional[str],
    recheck_delay: float,
    file_checks: dict,
) -> tuple[bool, Optional[str]]:
    reason = None
    for attempt in range(2):
        try:
            repomd = await fetch_repomd(
                repomd_url=repomd_url,
                http_session=http_session,
                sem=sem,
            )
        except RepodataError as err:
            reason = str(err)
            break
        reason = await check_repomd_files(
            repo_url=urljoin(repomd_url, '..'),
            repomd=repomd,
            check_types=check_types,
            http_session=http_session,
            sem=sem,
            logger=logger,
            file_checks=file_checks,
        )
        if reason is None:
            if (
                revisions is not None and
                revision_key is not None and
                repomd.revision is not None
            ):
                revisions[revision_key] = repomd.revision
            return True, None
        reason = f'repodata incomplete: {reason}'
        if attempt == 0:
            logger.info(
                'Checking "%s" again in %s seconds because "%s"',
                repomd_url,
                recheck_delay,
                reason,
            )
            await asyncio.sleep(recheck_delay)
    logger.warning(error_msg, {**error_msg_vars, 'err': reason})
    return False, reason


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
            repomd_check_types=[
                str(data_type) for data_type in yaml_data.get(
                    'repomd_check_types',
                    DEFAULT_REPOMD_CHECK_TYPES,
                )
            ],
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


def _repo_key(version: str, repo_path: str) -> str:
    """
    Identify a repo the same way on every mirror, e.g. "9.7/BaseOS/x86_64/os"
    """
    return f"{version}/{repo_path.strip('/')}"


def full_check_slot(now: Optional[float] = None) -> int:
    """
    Get which share of the repos has all its files checked in the run
    happening at now
    """
    if now is None:
        now = time.time()
    return int(now // FULL_CHECK_PERIOD) % FULL_CHECK_ROTATION


def get_check_types(
    main_config: MainConfig,
    repo_key: str,
    slot: int,
) -> list[str]:
    """
    Get the metadata types whose files are checked for a repo in this run:
    repomd_check_types, or all of them when it's the repo's turn
    :param slot: the run's full_check_slot()
    """
    # crc32, unlike hash(), gives the same result in every worker process
    if zlib.crc32(repo_key.encode()) % FULL_CHECK_ROTATION == slot:
        return ['*']
    return main_config.repomd_check_types


def get_repomd_urls(
    mirror_info: MirrorData,
    main_config: MainConfig,
) -> dict[str, dict[str, str]]:
    """
    Get the URL of repomd.xml of every repo a mirror is checked for
    :return: URL -> version, repo path and key of the repo
    """
    mirror_name = mirror_info.name
    urls = {}
    mirror_base = mirror_info.mirror_url.rstrip('/')
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
                url_for_check = (
                    f"{mirror_base}/{version}/"
                    f"{repo_path.strip('/')}/{REPOMD_PATH}"
                )
                urls[url_for_check] = {
                    'version': version,
                    'repo_path': repo_path,
                    'key': _repo_key(version, repo_path),
                }
    return urls


def get_module_repomd_urls(
    mirror_info: MirrorData,
    main_config: MainConfig,
    module: str,
) -> dict[str, dict[str, str]]:
    """
    Get the URL of repomd.xml of every repo of an optional module
    a mirror is checked for
    :return: URL -> version, repo path, key and module of the repo
    """
    urls = {}
    module_urls = mirror_info.module_urls[module]
    for ver in main_config.optional_module_versions[module]:
        version = f'{ver}-{module}'
        for repo_data in main_config.repos:
            repo_versions = repo_data.versions
            if repo_versions and version not in repo_versions:
                continue
            if repo_data.vault:
                continue
            arches = _get_arches_for_version(
                repo_arches=repo_data.arches,
                global_arches=main_config.arches[version],
            )
            for arch in arches:
                if not _is_permitted_arch_for_this_version_and_repo(
                    version=version,
                    arch=arch,
                    arches=main_config.arches,
                ):
                    continue
                repo_path = repo_data.path.replace('$basearch', arch)
                url_for_check = urljoin(
                    urljoin(
                        urljoin(
                            (
                                module_urls.get('http') or
                                module_urls.get('https')
                            ) + '/',
                            version,
                        ) + '/',
                        repo_path,
                    ) + '/',
                    REPOMD_PATH,
                )
                urls[url_for_check] = {
                    'version': version,
                    'repo_path': repo_path,
                    'key': _repo_key(version, repo_path),
                    'module': module,
                }
    return urls


async def mirror_available(
    mirror_info: MirrorData,
    http_session: ClientType,
    main_config: MainConfig,
    logger: Logger,
    revisions: Optional[dict[str, str]] = None,
) -> tuple[bool, Optional[str]]:
    """
    Check that a mirror serves usable metadata for every repo
    :param mirror_info: the dictionary which contains info about a mirror
                        (name, address, update frequency, sponsor info, email)
    :param logger: instance of Logger class
    :param main_config: main config of the mirrors service
    :param http_session: async HTTP session
    :param revisions: if given, gets the repomd.xml revision of every repo
                      that passed, keyed like "9.7/BaseOS/x86_64/os"
    """
    mirror_name = mirror_info.name
    logger.info('Checking mirror "%s"...', mirror_name)
    if mirror_info.private:
        logger.info(
            'Mirror "%s" is private and won\'t be checked',
            mirror_name,
        )
        return True, None
    error_msg = (
        'Mirror "%(name)s" is not available for version '
        '"%(version)s" and repo path "%(repo)s" because "%(err)s"'
    )
    mirror_availability_semaphore = asyncio.Semaphore(
        REPO_CHECK_CONCURRENCY
    )
    file_checks = {}
    slot = full_check_slot()
    tasks = [asyncio.ensure_future(
        check_repo_metadata(
            repomd_url=check_url,
            http_session=http_session,
            logger=logger,
            sem=mirror_availability_semaphore,
            check_types=get_check_types(
                main_config=main_config,
                repo_key=url_info['key'],
                slot=slot,
            ),
            error_msg=error_msg,
            error_msg_vars={
                'name': mirror_name,
                'version': url_info['version'],
                'repo': url_info['repo_path'],
            },
            revisions=revisions,
            revision_key=url_info['key'],
            file_checks=file_checks,
        )
    ) for check_url, url_info in get_repomd_urls(
        mirror_info=mirror_info,
        main_config=main_config,
    ).items()]
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
    module: str,
    revisions: Optional[dict[str, str]] = None,
):
    """
    Check that a mirror serves usable metadata for every repo of an optional
    module, and add the module to the mirror's has_optional_modules if so
    :param revisions: if given, gets the repomd.xml revision of every repo
                      that passed, keyed like "10-kitten/BaseOS/x86_64/os"
    """
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

    error_msg = (
        'Mirror "%(name)s" optional module "%(module)s" '
        'is not available for version '
        '"%(version)s" and repo path "%(repo)s" because "%(err)s"'
    )
    
    mirror_availability_semaphore = asyncio.Semaphore(
        REPO_CHECK_CONCURRENCY
    )
    file_checks = {}
    slot = full_check_slot()
    tasks = [asyncio.ensure_future(
        check_repo_metadata(
            repomd_url=check_url,
            http_session=http_session,
            logger=logger,
            sem=mirror_availability_semaphore,
            check_types=get_check_types(
                main_config=main_config,
                repo_key=url_info['key'],
                slot=slot,
            ),
            error_msg=error_msg,
            error_msg_vars={
                'name': mirror_name,
                'version': url_info['version'],
                'repo': url_info['repo_path'],
                'module': url_info['module'],
            },
            revisions=revisions,
            revision_key=url_info['key'],
            file_checks=file_checks,
        )
    ) for check_url, url_info in get_module_repomd_urls(
        mirror_info=mirror_info,
        main_config=main_config,
        module=module,
    ).items()]
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
