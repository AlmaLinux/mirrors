# coding=utf-8
import os
from typing import AnyStr, List

import dateparser

from api.exceptions import BadRequestFormatExceptioin
from api.mirrors_update import (
    get_config,
    get_verified_mirrors,
    REQUIRED_MIRROR_PROTOCOLS, get_mirrors_info,
)
from api.utils import get_geo_data_by_ip
from db.models import Url, Mirror
from db.utils import session_scope
from sqlalchemy.sql.expression import false

from common.sentry import (
    get_logger,
)


MAX_LENGTH_OF_MIRRORS_LIST = 5

logger = get_logger(__name__)


def _get_nearest_mirrors(ip_address):
    """
    The function returns 5 nearest mirrors towards a request's IP
    Firstly, it searches first 5 mirrors inside a request's country
    Secondly, it searches first 5 nearest mirrors by distance
        inside a request's continent
    Thirdly, it searches first 5 nearest mirrors by distance in the world
    Further the functions concatenate lists and return first
        5 elements of a summary list
    """
    ip_address = '87.240.190.67'
    match = get_geo_data_by_ip(ip_address)
    with session_scope() as session:
        all_mirrors_query = session.query(Mirror).filter(
            Mirror.is_expired == false(),
        )
        # We return all of mirrors if we can't
        # determine geo data of a request's IP
        if match is None:
            all_mirrors = [
                mirror.to_dict() for mirror in all_mirrors_query.all()
            ]
            return all_mirrors
        continent, country, latitude, longitude = match
        # get five mirrors in a request's country
        mirrors_by_country_query = session.query(Mirror).filter(
            Mirror.continent == continent,
            Mirror.country == country,
            Mirror.is_expired == false(),
        ).limit(
            MAX_LENGTH_OF_MIRRORS_LIST,
        )
        # get five nearest mirrors inside a request's continent
        mirrors_by_continent_query = session.query(Mirror).filter(
            Mirror.continent == continent,
            Mirror.is_expired == false(),
            ).order_by(
            Mirror.conditional_distance(
                lon=longitude,
                lat=latitude,
            )
        ).limit(
            MAX_LENGTH_OF_MIRRORS_LIST,
        )
        # get five nearest mirrors from all of mirrors
        all_rest_mirrors_query = session.query(Mirror).filter(
            Mirror.is_expired == false(),
            ).order_by(
            Mirror.conditional_distance(
                lon=longitude,
                lat=latitude,
            )
        ).limit(
            MAX_LENGTH_OF_MIRRORS_LIST,
        )
        suitable_mirrors_query = mirrors_by_country_query.union_all(
            mirrors_by_continent_query,
        ).union_all(
            all_rest_mirrors_query,
        )
        suitable_mirrors = suitable_mirrors_query.all()
        # return five nearst mirrors
        suitable_mirrors = [mirror.to_dict() for mirror
                            in suitable_mirrors][:MAX_LENGTH_OF_MIRRORS_LIST]
        return suitable_mirrors


def update_mirrors_handler():
    config = get_config()
    versions = config['versions']
    repos = config['repos']
    mirrors_dir=os.path.join(
        os.path.dirname(
            os.path.abspath(__file__),
        ),
        '../../../mirrors',
        config['mirrors_dir'],
    )
    all_mirrors = get_mirrors_info(
        mirrors_dir=mirrors_dir,
    )
    verified_mirrors = get_verified_mirrors(
        all_mirrors=all_mirrors,
        versions=versions,
        repos=repos,
        allowed_outdate=config['allowed_outdate']
    )
    with session_scope() as session:
        session.query(Mirror).delete()
        session.query(Url).delete()
        for mirror in verified_mirrors:
            urls_to_create = [
                Url(
                    url=url,
                    type=url_type,
                ) for url_type, url in mirror['address'].items()
            ]
            for url_to_create in urls_to_create:
                session.add(url_to_create)
            mirror_to_create = Mirror(
                name=mirror['name'],
                continent=mirror['continent'],
                country=mirror['country'],
                ip=mirror['ip'],
                latitude=mirror['location']['lat'],
                longitude=mirror['location']['lon'],
                is_expired=mirror['status'] == 'expired',
                update_frequency=dateparser.parse(mirror['update_frequency']),
                sponsor_name=mirror['sponsor'],
                sponsor_url=mirror['sponsor_url'],
                email=mirror['email'],
                urls=urls_to_create,
            )
            session.add(mirror_to_create)
        session.flush()

    return 'Done'


def get_all_mirrors():
    mirrors_list = []
    with session_scope() as session:
        mirrors = session.query(Mirror).all()
        for mirror in mirrors:
            mirror_data = mirror.to_dict()
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
        raise BadRequestFormatExceptioin(
            'Unknown version "%s". Allowed list of versions "%s"',
            version,
            ', '.join(versions),
        )
    repos = {repo['name']: repo['path'] for repo in config['repos']}
    if repository not in repos:
        raise BadRequestFormatExceptioin(
            'Unknown repository "%s". Allowed list of repositories "%s"',
            repository,
            ', '.join(repos.keys()),
        )
    repo_path = repos[repository]
    nearest_mirrors = _get_nearest_mirrors(ip_address=ip_address)
    for mirror in nearest_mirrors:
        mirror_url = mirror['urls'].get(REQUIRED_MIRROR_PROTOCOLS[0]) or \
                     mirror['urls'].get(REQUIRED_MIRROR_PROTOCOLS[1])
        full_mirror_path = os.path.join(
            mirror_url,
            version,
            repo_path
        )
        mirrors_list.append(full_mirror_path)

    return '\n'.join(mirrors_list)


def get_url_types() -> List[AnyStr]:
    with session_scope() as session:
        return [value[0] for value in session.query(
            Url.type
        ).distinct()]
