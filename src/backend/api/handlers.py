# coding=utf-8
import asyncio
import itertools
import os
import random
from collections import defaultdict
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

import dateparser
from sqlalchemy.orm import joinedload

from api.exceptions import UnknownRepoAttribute
from api.mirror_processor import MirrorProcessor
from yaml_snippets.utils import (
    get_config,
    get_mirrors_info,
)
from api.redis import (
    set_mirrors_to_cache,
    get_url_types_from_cache,
    set_url_types_to_cache,
    set_mirror_list,
    get_mirror_list,
    get_mirrors_from_cache,
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
    Mirror,
    get_asn_by_ip,
    is_ip_in_any_subnet,
    Subnet,
)
from db.utils import session_scope
from sqlalchemy.sql.expression import or_
from common.sentry import get_logger

logger = get_logger(__name__)


LENGTH_GEO_MIRRORS_LIST = 10
LENGTH_CLOUD_MIRRORS_LIST = 5
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


async def _get_nearest_mirrors_by_network_data(
        ip_address: str,
        get_without_private_mirrors: bool,
        get_without_cloud_mirrors: bool,
        get_mirrors_with_full_set_of_isos: bool,
        get_working_mirrors: bool,
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
        return mirror_data.status == 'ok' and \
            not mirror_data.private and \
            mirror_data not in main_list_of_mirrors

    match = get_geo_data_by_ip(ip_address)
    asn = get_asn_by_ip(ip_address)
    suitable_mirrors = []

    mirrors = await get_all_mirrors(
        get_working_mirrors=get_working_mirrors,
        get_without_cloud_mirrors=get_without_cloud_mirrors,
        get_without_private_mirrors=get_without_private_mirrors,
        get_mirrors_with_full_set_of_isos=get_mirrors_with_full_set_of_isos,
    )
    for mirror in mirrors:
        if mirror.status != "ok":
            continue
        if (asn is not None and asn in mirror.asn) or is_ip_in_any_subnet(
            ip_address=ip_address,
            subnets=mirror.subnets,
        ):
            if mirror.monopoly:
                return [mirror]
            else:
                suitable_mirrors.append(mirror)
    if 1 <= len(suitable_mirrors) < LENGTH_CLOUD_MIRRORS_LIST\
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


async def _get_nearest_mirrors_by_geo_data(
        ip_address: str,
        get_without_private_mirrors: bool,
        get_without_cloud_mirrors: bool,
        get_mirrors_with_full_set_of_isos: bool,
        get_working_mirrors: bool,
) -> list[MirrorData]:
    """
    The function returns nearest N mirrors to a client
    Read comments below to get more information
    """
    match = get_geo_data_by_ip(ip_address)
    mirrors = await get_all_mirrors(
        get_working_mirrors=get_working_mirrors,
        get_without_cloud_mirrors=get_without_cloud_mirrors,
        get_without_private_mirrors=get_without_private_mirrors,
        get_mirrors_with_full_set_of_isos=get_mirrors_with_full_set_of_isos,
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


async def _get_nearest_mirrors(
        ip_address: Optional[str],
        get_without_private_mirrors: bool,
        get_without_cloud_mirrors: bool,
        get_mirrors_with_full_set_of_isos: bool,
        get_working_mirrors: bool,
) -> list[MirrorData]:
    """
    Get the nearest mirrors by geo-data or by subnet/ASN
    """
    if ip_address is None:
        return await get_all_mirrors(
            get_working_mirrors=get_working_mirrors,
            get_without_cloud_mirrors=get_without_cloud_mirrors,
            get_without_private_mirrors=get_without_private_mirrors,
            get_mirrors_with_full_set_of_isos=get_mirrors_with_full_set_of_isos,
        )
    if os.getenv('DISABLE_CACHING_NEAREST_MIRRORS'):
        suitable_mirrors = None
    else:
        suitable_mirrors = await get_mirrors_from_cache(
            key=ip_address,
            get_mirrors_with_full_set_of_isos=get_mirrors_with_full_set_of_isos
        )
    if suitable_mirrors is not None:
        return suitable_mirrors
    suitable_mirrors = await _get_nearest_mirrors_by_network_data(
        ip_address=ip_address,
        get_working_mirrors=get_working_mirrors,
        get_without_cloud_mirrors=get_without_cloud_mirrors,
        get_without_private_mirrors=get_without_private_mirrors,
        get_mirrors_with_full_set_of_isos=get_mirrors_with_full_set_of_isos,
    )
    if not suitable_mirrors:
        suitable_mirrors = await _get_nearest_mirrors_by_geo_data(
            ip_address=ip_address,
            get_working_mirrors=get_working_mirrors,
            get_without_cloud_mirrors=get_without_cloud_mirrors,
            get_without_private_mirrors=get_without_private_mirrors,
            get_mirrors_with_full_set_of_isos=get_mirrors_with_full_set_of_isos
        )
    await set_mirrors_to_cache(
        key=ip_address,
        mirrors=suitable_mirrors,
        get_mirrors_with_full_set_of_isos=get_mirrors_with_full_set_of_isos,
    )
    return suitable_mirrors


async def update_mirrors_handler() -> str:

    main_config = get_config(
        logger=logger,
        path_to_config=SERVICE_CONFIG_PATH,
        path_to_json_schema=SERVICE_CONFIG_JSON_SCHEMA_DIR_PATH,
    )
    mirrors_dir = os.path.join(
        os.getenv('CONFIG_ROOT'),
        'mirrors/updates',
        main_config.mirrors_dir,
    )
    all_mirrors = get_mirrors_info(
        mirrors_dir=mirrors_dir,
        logger=logger,
        main_config=main_config,
        path_to_json_schema=MIRROR_CONFIG_JSON_SCHEMA_DIR_PATH,
    )

    pid_file_path = Path(os.getenv('MIRRORS_UPDATE_PID'))
    if pid_file_path.exists():
        return 'Update is already running'
    try:
        pid_file_path.write_text(str(os.getpid()))
        step = 20
        mirror_check_sem = asyncio.Semaphore(step)
        mirrors_len = len(all_mirrors)
        async with mirror_check_sem, MirrorProcessor(
            logger=logger,
        ) as mirror_processor:  # type: MirrorProcessor
            mirror_iso_uris = mirror_processor.get_mirror_iso_uris(
                versions=set(main_config.versions) -
                set(main_config.duplicated_versions),
                arches=main_config.arches
            )
            subnets = await get_aws_subnets(
                http_session=mirror_processor.client,
            )
            subnets.update(
                await get_azure_subnets(
                    http_session=mirror_processor.client,
                ),
            )
            for i in range(0, mirrors_len, step):
                next_slice = min(i + step, mirrors_len)
                await asyncio.gather(*(
                    asyncio.ensure_future(
                        mirror_processor.set_subnets_for_cloud_mirror(
                            subnets=subnets,
                            mirror_info=mirror_info,
                        )
                    ) for mirror_info in all_mirrors[i:next_slice]
                    if mirror_info.cloud_type
                ))
                await asyncio.gather(*(
                    asyncio.ensure_future(
                        mirror_processor.set_ip_for_mirror(
                            mirror_info=mirror_info,
                        )
                    ) for mirror_info in all_mirrors[i:next_slice]
                ))
                await asyncio.gather(*(
                    asyncio.ensure_future(
                        mirror_processor.set_iso_url(
                            mirror_info=mirror_info,
                        )
                    ) for mirror_info in all_mirrors[i:next_slice]
                ))
                await asyncio.gather(*(
                    asyncio.ensure_future(
                        mirror_processor.set_status_of_mirror(
                            main_config=main_config,
                            mirror_info=mirror_info,
                        )
                    ) for mirror_info in all_mirrors[i:next_slice]
                    if mirror_info.ip not in ('Unknown', None)
                ))
                await asyncio.gather(*(
                    asyncio.ensure_future(
                        mirror_processor.set_ipv6_support_of_mirror(
                            mirror_info=mirror_info,
                        )
                    ) for mirror_info in all_mirrors[i:next_slice]
                    if mirror_info.ip not in ('Unknown', None)
                    and not mirror_info.private
                ))
                await asyncio.gather(*(
                    asyncio.ensure_future(
                        mirror_processor.set_geo_and_location_data_from_db(
                            mirror_info=mirror_info)
                    ) for mirror_info in all_mirrors[i:next_slice]
                    if mirror_info.status in ('ok', 'expired')
                ))
                await asyncio.gather(*(
                    asyncio.ensure_future(
                        mirror_processor.set_mirror_have_full_iso_set(
                            mirror_info=mirror_info,
                            mirror_iso_uris=mirror_iso_uris,
                        )
                    ) for mirror_info in all_mirrors[i:next_slice]
                    if mirror_info.status in ('ok', 'expired')
                    and mirror_info.ip not in ('Unknown', None)
                    and not mirror_info.private
                    and mirror_info.cloud_type in ('', None)
                ))
                await asyncio.gather(*(
                    asyncio.ensure_future(
                        mirror_processor.set_location_data_from_online_service(
                            mirror_info=mirror_info)
                    ) for mirror_info in all_mirrors[i:next_slice]
                    if mirror_info.status in ('ok', 'expired')
                ))
        with session_scope() as db_session:
            db_session.query(Mirror).delete()
            db_session.query(Url).delete()
            db_session.query(Subnet).delete()
            for mirror_info in all_mirrors:
                if mirror_info.ip in ('Unknown', None):
                    continue
                if mirror_info.status not in ('ok', 'expired'):
                    continue
                urls_to_create = [
                    Url(
                        url=url,
                        type=url_type,
                    ) for url_type, url in mirror_info.urls.items()
                ]
                db_session.add_all(urls_to_create)
                mirror_to_create = Mirror(
                    name=mirror_info.name,
                    continent=mirror_info.geolocation.continent,
                    country=mirror_info.geolocation.country,
                    state=mirror_info.geolocation.state,
                    city=mirror_info.geolocation.city,
                    ip=mirror_info.ip,
                    ipv6=mirror_info.ipv6,
                    latitude=mirror_info.location.latitude,
                    longitude=mirror_info.location.longitude,
                    status=mirror_info.status,
                    update_frequency=dateparser.parse(
                        mirror_info.update_frequency
                    ),
                    sponsor_name=mirror_info.sponsor_name,
                    mirror_url=mirror_info.mirror_url,
                    iso_url=mirror_info.iso_url,
                    sponsor_url=mirror_info.sponsor_url,
                    email=mirror_info.email,
                    cloud_type=mirror_info.cloud_type,
                    cloud_region=mirror_info.cloud_region,
                    urls=urls_to_create,
                    private=mirror_info.private,
                    monopoly=mirror_info.monopoly,
                    asn=','.join(mirror_info.asn),
                    has_full_iso_set=mirror_info.has_full_iso_set,
                )
                if mirror_info.subnets:
                    subnets_to_create = [
                        Subnet(
                            subnet=subnet,
                        ) for subnet in mirror_info.subnets
                    ]
                    db_session.add_all(subnets_to_create)
                    mirror_to_create.subnets = subnets_to_create
                db_session.add(mirror_to_create)
        # update all mirrors list in the Redis cache
        for args in itertools.product((True, False), repeat=4):
            await refresh_mirrors_cache(*args)
    finally:
        if pid_file_path.exists():
            os.remove(pid_file_path)
    return 'Done'


async def refresh_mirrors_cache(
        get_working_mirrors: bool = False,
        get_without_cloud_mirrors: bool = False,
        get_without_private_mirrors: bool = False,
        get_mirrors_with_full_set_of_isos: bool = False
):
    """
    Refresh cache of a mirrors list in Redis
    :param get_working_mirrors: select mirrors which are not expired
    :param get_without_cloud_mirrors: select mirrors without those who are
           hosted in clouds (Azure/AWS)
    :param get_without_private_mirrors: select mirrors without those who are
           hosted behind NAT
    :param get_mirrors_with_full_set_of_isos: select mirrors which have full
           set of ISOs and them artifacts (CHECKSUM, manifests)
           per each version and architecture
    """
    mirrors = await get_all_mirrors_db(
        get_working_mirrors=get_working_mirrors,
        get_without_cloud_mirrors=get_without_cloud_mirrors,
        get_without_private_mirrors=get_without_private_mirrors,
        get_mirrors_with_full_set_of_isos=get_mirrors_with_full_set_of_isos,
    )
    mirror_list = [mirror.to_json() for mirror in mirrors]
    await set_mirror_list(
        mirrors=mirror_list,
        get_working_mirrors=get_working_mirrors,
        get_without_cloud_mirrors=get_without_cloud_mirrors,
        get_without_private_mirrors=get_without_private_mirrors,
        get_mirrors_with_full_set_of_isos=get_mirrors_with_full_set_of_isos,
    )


async def get_all_mirrors(
        get_working_mirrors: bool = False,
        get_without_cloud_mirrors: bool = False,
        get_without_private_mirrors: bool = False,
        get_mirrors_with_full_set_of_isos: bool = False
) -> list[MirrorData]:
    """
    Get the list of all mirrors from cache or regenerate one if it's empty
    :param get_working_mirrors: select mirrors which are not expired
    :param get_without_cloud_mirrors: select mirrors without those who are
           hosted in clouds (Azure/AWS)
    :param get_without_private_mirrors: select mirrors without those who are
           hosted behind NAT
    :param get_mirrors_with_full_set_of_isos: select mirrors which have full
           set of ISOs and them artifacts (CHECKSUM, manifests)
           per each version and architecture
    """
    mirrors = await get_mirror_list(
        get_working_mirrors=get_working_mirrors,
        get_without_cloud_mirrors=get_without_cloud_mirrors,
        get_without_private_mirrors=get_without_private_mirrors,
        get_mirrors_with_full_set_of_isos=get_mirrors_with_full_set_of_isos,
    )
    if not mirrors:
        await refresh_mirrors_cache(
            get_working_mirrors=get_working_mirrors,
            get_without_cloud_mirrors=get_without_cloud_mirrors,
            get_without_private_mirrors=get_without_private_mirrors,
            get_mirrors_with_full_set_of_isos=get_mirrors_with_full_set_of_isos
        )
        mirrors = await get_mirror_list(
            get_working_mirrors=get_working_mirrors,
            get_without_cloud_mirrors=get_without_cloud_mirrors,
            get_without_private_mirrors=get_without_private_mirrors,
            get_mirrors_with_full_set_of_isos=get_mirrors_with_full_set_of_isos
        )
    random.shuffle(mirrors)
    return mirrors


async def get_all_mirrors_db(
        get_working_mirrors: bool = False,
        get_without_cloud_mirrors: bool = False,
        get_without_private_mirrors: bool = False,
        get_mirrors_with_full_set_of_isos: bool = False
) -> list[MirrorData]:
    """
    Get a mirrors list from DB
    :param get_working_mirrors: select mirrors which are not expired
    :param get_without_cloud_mirrors: select mirrors without those who are
           hosted in clouds (Azure/AWS)
    :param get_without_private_mirrors: select mirrors without those who are
           hosted behind NAT
    :param get_mirrors_with_full_set_of_isos: select mirrors which have full
           set of ISOs and them artifacts (CHECKSUM, manifests)
           per each version and architecture
    """
    mirrors_list = []
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
        if get_working_mirrors:
            mirrors_query = mirrors_query.filter(
                Mirror.status == 'ok',
            )
        if get_without_cloud_mirrors:
            mirrors_query = mirrors_query.filter(
                Mirror.cloud_type == '',
            )
        mirrors = mirrors_query.all()
        for mirror in mirrors:
            mirror_data = mirror.to_dataclass()
            mirrors_list.append(mirror_data)
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


async def get_mirrors_list(
        ip_address: Optional[str],
        version: str,
        arch: Optional[str],
        repository: Optional[str],
        iso_list: bool = False,
) -> str:
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
        return os.path.join(
            vault_mirror,
            version,
            repo_path,
        )
    if iso_list:
        nearest_mirrors = await _get_nearest_mirrors(
            ip_address=ip_address,
            get_mirrors_with_full_set_of_isos=True,
            get_without_private_mirrors=True,
            get_working_mirrors=True,
            get_without_cloud_mirrors=True,
        )
    else:
        nearest_mirrors = await _get_nearest_mirrors(
            ip_address=ip_address,
            get_mirrors_with_full_set_of_isos=False,
            get_without_private_mirrors=False,
            get_working_mirrors=True,
            get_without_cloud_mirrors=False,
        )
    for mirror in nearest_mirrors:
        full_mirror_path = urljoin(
            mirror.mirror_url + '/',
            f'{version}/{repo_path}',
        )
        mirrors_list.append(full_mirror_path)

    return '\n'.join(mirrors_list)


async def get_isos_list_by_countries(
        ip_address: Optional[str],
) -> tuple[dict[str, list[MirrorData]], list[MirrorData]]:
    mirrors_by_countries = defaultdict(list)
    for mirror_info in await get_all_mirrors(
        get_without_private_mirrors=True,
        get_mirrors_with_full_set_of_isos=True,
        get_without_cloud_mirrors=True,
        get_working_mirrors=True,
    ):
        mirrors_by_countries[
            mirror_info.geolocation.country
        ].append(mirror_info)
    nearest_mirrors = await _get_nearest_mirrors(
        ip_address=ip_address,
        get_without_private_mirrors=True,
        get_mirrors_with_full_set_of_isos=True,
        get_without_cloud_mirrors=True,
        get_working_mirrors=True,
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


async def get_url_types() -> list[str]:
    url_types = await get_url_types_from_cache()
    if url_types is not None:
        return url_types
    with session_scope() as session:
        url_types = sorted(value[0] for value in session.query(
            Url.type
        ).distinct())
        await set_url_types_to_cache(url_types)
        return url_types
