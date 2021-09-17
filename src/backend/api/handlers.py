# coding=utf-8
import asyncio
import os
from collections import defaultdict
from typing import (
    AnyStr,
    List,
    Dict,
    Tuple,
    Union,
)

from aiohttp import ClientSession
from sqlalchemy.orm import Session

from api.exceptions import UnknownRepositoryOrVersion
from api.mirrors_update import (
    get_config,
    REQUIRED_MIRROR_PROTOCOLS,
    get_mirrors_info,
    ARCHS,
    update_mirror_in_db,
)
from api.redis import (
    get_mirrors_from_cache,
    set_mirrors_to_cache,
    get_url_types_from_cache,
    set_url_types_to_cache,
)
from api.utils import (
    get_geo_data_by_ip,
    get_aws_subnets,
    get_azure_subnets,
    set_subnets_for_hyper_cloud_mirror,
)
from db.models import (
    Url,
    Mirror,
    MirrorData,
    get_asn_by_ip,
    is_ip_in_any_subnet,
    Subnet,
    mirrors_subnets,
    mirrors_urls,
    MirrorYamlData,
)
from db.utils import session_scope
from sqlalchemy.sql.expression import (
    null,
    false,
)
from common.sentry import (
    get_logger,
)

logger = get_logger(__name__)


LENGTH_GEO_MIRRORS_LIST = 10
LENGTH_CLOUD_MIRRORS_LIST = 5


def _get_nearest_mirrors_by_network_data(
        ip_address: AnyStr,
) -> List[MirrorData]:
    """
    The function returns mirrors which are in the same subnet or have the same
    ASN as a request's IP
    """

    match = get_geo_data_by_ip(ip_address)
    asn = get_asn_by_ip(ip_address)
    suitable_mirrors = []
    with session_scope() as session:
        mirrors = session.query(Mirror).filter(
            (Mirror.asn != null()) | (Mirror.subnets != null())
        ).all()
        for mirror in mirrors:
            if (asn and asn == mirror.asn) or is_ip_in_any_subnet(
                ip_address=ip_address,
                subnets=mirror.get_subnets(),
            ):
                suitable_mirrors.append(mirror.to_dataclass())
        if 1 <= len(suitable_mirrors) < LENGTH_CLOUD_MIRRORS_LIST\
                and match is not None:
            continent, country, latitude, longitude = match
            nearest_mirrors = session.query(Mirror).filter(
                Mirror.name.not_in([mirror.name for mirror in
                                    suitable_mirrors])
            ).order_by(
                Mirror.conditional_distance(
                    lon=longitude,
                    lat=latitude,
                )
            ).limit(
                LENGTH_CLOUD_MIRRORS_LIST - len(suitable_mirrors)
            )  # type: List[Mirror]
            suitable_mirrors.extend(
                mirror.to_dataclass() for mirror in nearest_mirrors
            )
        return suitable_mirrors


def _get_nearest_mirrors_by_geo_data(
        ip_address: AnyStr,
        empty_for_unknown_ip: bool = False,
) -> List[MirrorData]:
    """
    The function returns N nearest mirrors towards a request's IP
    Firstly, it searches first N mirrors inside a request's country
    Secondly, it searches first N nearest mirrors by distance
        inside a request's continent
    Thirdly, it searches first N nearest mirrors by distance in the world
    Further the functions concatenate lists and return first
        N elements of a summary list
    :param empty_for_unknown_ip: if True and we can't get geo data of an IP
        the function returns empty list
    """
    match = get_geo_data_by_ip(ip_address)
    with session_scope() as session:
        all_mirrors_query = session.query(Mirror).filter(
            Mirror.is_expired == false(),
            )
        if empty_for_unknown_ip:
            all_mirrors_query = session.query(Mirror).filter(
                Mirror.cloud_type == null(),
            )
        # We return all of mirrors if we can't
        # determine geo data of a request's IP
        if match is None:
            all_mirrors = [] if empty_for_unknown_ip else [
                mirror.to_dataclass() for mirror in all_mirrors_query.all()
            ]
            return all_mirrors
        continent, country, latitude, longitude = match
        # get n-mirrors in a request's country
        mirrors_by_country_query = session.query(Mirror).filter(
            Mirror.continent == continent,
            Mirror.country == country,
            Mirror.is_expired == false(),
            ).limit(
            LENGTH_GEO_MIRRORS_LIST,
        )
        # get n-mirrors mirrors inside a request's continent
        # but outside a request's contry
        mirrors_by_continent_query = session.query(Mirror).filter(
            Mirror.continent == continent,
            Mirror.country != country,
            Mirror.is_expired == false(),
            ).order_by(
            Mirror.conditional_distance(
                lon=longitude,
                lat=latitude,
            )
        ).limit(
            LENGTH_GEO_MIRRORS_LIST,
        )
        # get n-mirrors mirrors from all of mirrors outside
        # a request's country and continent
        all_rest_mirrors_query = session.query(Mirror).filter(
            Mirror.is_expired == false(),
            Mirror.continent != continent,
            Mirror.country != country,
            ).order_by(
            Mirror.conditional_distance(
                lon=longitude,
                lat=latitude,
            )
        ).limit(
            LENGTH_GEO_MIRRORS_LIST,
        )

        # TODO: SQLAlchemy adds brackets around queries. And it looks like
        # TODO: incorrect query for SQLite
        # suitable_mirrors_query = mirrors_by_country_query.union_all(
        #     mirrors_by_continent_query,
        # ).union_all(
        #     all_rest_mirrors_query,
        # ).limit(MAX_LENGTH_OF_MIRRORS_LIST)
        # suitable_mirrors = suitable_mirrors_query.all()

        # return n-nearest mirrors
        mirrors_by_country = mirrors_by_country_query.all()
        mirrors_by_continent = mirrors_by_continent_query.all()
        all_rest_mirrors = all_rest_mirrors_query.all()

        suitable_mirrors = mirrors_by_country + \
            mirrors_by_continent + \
            all_rest_mirrors
        suitable_mirrors = [mirror.to_dataclass() for mirror
                            in suitable_mirrors[:LENGTH_GEO_MIRRORS_LIST]]
    return suitable_mirrors


def _get_nearest_mirrors(
        ip_address: AnyStr,
        empty_for_unknown_ip: bool = False,
) -> List[MirrorData]:
    """
    Get nearest mirrors by geo-data or by subnet/ASN
    """
    if os.environ.get('DEPLOY_ENVIRONMENT').lower() in (
        'dev',
        'development',
    ):
        ip_address = os.environ.get(
            'TEST_IP_ADDRESS',
        ) or '195.123.213.149'
    suitable_mirrors = get_mirrors_from_cache(ip_address)
    if suitable_mirrors is not None:
        return suitable_mirrors
    suitable_mirrors = _get_nearest_mirrors_by_network_data(
        ip_address=ip_address,
    )
    if not suitable_mirrors:
        suitable_mirrors = _get_nearest_mirrors_by_geo_data(
            ip_address=ip_address,
            empty_for_unknown_ip=empty_for_unknown_ip,
        )
    set_mirrors_to_cache(
        ip_address,
        suitable_mirrors,
    )
    return suitable_mirrors


async def _process_mirror(
        subnets: Dict[AnyStr, List[AnyStr]],
        mirror_info: MirrorYamlData,
        versions: List[AnyStr],
        repos: List[Dict[AnyStr, Union[Dict, AnyStr]]],
        allowed_outdate: AnyStr,
        db_session: Session,
        http_session: ClientSession,
):
    set_subnets_for_hyper_cloud_mirror(
        subnets=subnets,
        mirror_info=mirror_info,

    )
    await update_mirror_in_db(
        mirror_info=mirror_info,
        versions=versions,
        repos=repos,
        allowed_outdate=allowed_outdate,
        db_session=db_session,
        http_session=http_session,
    )


async def update_mirrors_handler() -> AnyStr:
    config = get_config()
    versions = config['versions']
    repos = config['repos']
    mirrors_dir = os.path.join(
        os.getenv('CONFIG_ROOT'),
        'mirrors/updates',
        config['mirrors_dir'],
    )
    all_mirrors = get_mirrors_info(
        mirrors_dir=mirrors_dir,
    )

    with session_scope() as db_session:
        db_session.query(Mirror).delete()
        db_session.query(Subnet).delete()
        db_session.query(mirrors_urls).delete()
        db_session.query(mirrors_subnets).delete()
        subnets = get_aws_subnets()
        subnets.update(get_azure_subnets())
        len_list = len(all_mirrors)
        procs = 30
        async with ClientSession() as http_session:
            for start in range(0, len_list + 1, procs):
                end = start + procs if start + procs <= len_list else len_list
                await asyncio.gather(*(
                    asyncio.ensure_future(
                        _process_mirror(
                            subnets=subnets,
                            mirror_info=mirror_info,
                            versions=versions, repos=repos,
                            allowed_outdate=config[
                                'allowed_outdate'],
                            db_session=db_session,
                            http_session=http_session,
                        )
                    ) for mirror_info in all_mirrors[start:end]
                ))
        db_session.flush()

    return 'Done'


def get_all_mirrors() -> List[MirrorData]:
    mirrors_list = []
    with session_scope() as session:
        mirrors = session.query(
            Mirror
        ).order_by(
            Mirror.continent,
            Mirror.country,
        ).all()
        for mirror in mirrors:
            mirror_data = mirror.to_dataclass()
            mirrors_list.append(mirror_data)
    return mirrors_list


def get_mirrors_list(
        ip_address: AnyStr,
        version: AnyStr,
        repository: AnyStr,
) -> AnyStr:
    mirrors_list = []
    config = get_config()
    versions = [str(version) for version in config['versions']]
    if version not in versions:
        try:
            version = next(ver for ver in versions if version.startswith(ver))
        except StopIteration:
            raise UnknownRepositoryOrVersion(
                'Unknown version "%s". Allowed list of versions "%s"',
                version,
                ', '.join(versions),
            )
    repos = {repo['name']: repo['path'] for repo in config['repos']}
    if repository not in repos:
        raise UnknownRepositoryOrVersion(
            'Unknown repository "%s". Allowed list of repositories "%s"',
            repository,
            ', '.join(repos.keys()),
        )
    repo_path = repos[repository]
    nearest_mirrors = _get_nearest_mirrors(ip_address=ip_address)
    for mirror in nearest_mirrors:
        mirror_url = mirror.urls.get(REQUIRED_MIRROR_PROTOCOLS[0]) or \
                     mirror.urls.get(REQUIRED_MIRROR_PROTOCOLS[1])
        full_mirror_path = os.path.join(
            mirror_url,
            version,
            repo_path
        )
        mirrors_list.append(full_mirror_path)

    return '\n'.join(mirrors_list)


def _set_isos_link_for_mirror(
        mirror_info: MirrorData,
        version: AnyStr,
        arch: AnyStr,
):
    urls = mirror_info.urls
    mirror_url = next(
        address for protocol_type, address in
        urls.items()
        if protocol_type in REQUIRED_MIRROR_PROTOCOLS
    )
    mirror_info.isos_link = os.path.join(
        mirror_url,
        str(version),
        'isos',
        arch,
    )


def get_isos_list_by_countries(
        arch: AnyStr,
        version: AnyStr,
        ip_address: AnyStr,
) -> Tuple[Dict[AnyStr, List[MirrorData]], List[MirrorData]]:
    mirrors_by_countries = defaultdict(list)
    for mirror_info in get_all_mirrors():
        # Hyper clouds (like AWS/Azure) don't have isos, because they traffic
        # is too expensive
        if mirror_info.cloud_type in ('aws', 'azure'):
            continue

        _set_isos_link_for_mirror(
            mirror_info=mirror_info,
            version=version,
            arch=arch
        )
        mirrors_by_countries[mirror_info.country].append(mirror_info)
    nearest_mirrors = _get_nearest_mirrors(
        ip_address=ip_address,
        empty_for_unknown_ip=True,
    )
    for nearest_mirror in nearest_mirrors:
        _set_isos_link_for_mirror(
            mirror_info=nearest_mirror,
            version=version,
            arch=arch
        )
    return mirrors_by_countries, nearest_mirrors


def get_main_isos_table() -> Dict[AnyStr, List[AnyStr]]:
    result = defaultdict(list)
    config = get_config()
    versions = config['versions']
    duplicated_versions = config['duplicated_versions']
    for arch in ARCHS:
        result[arch] = [version for version in versions
                        if version not in duplicated_versions]

    return result


def get_url_types() -> List[AnyStr]:
    url_types = get_url_types_from_cache()
    if url_types is not None:
        return url_types
    with session_scope() as session:
        url_types = sorted(value[0] for value in session.query(
            Url.type
        ).distinct())
        set_url_types_to_cache(url_types)
        return url_types
