#!/usr/bin/env python3.9
import argparse
import logging

import yaml
import json
import requests

from aiohttp import (
    TCPConnector,
    ClientSession,
)
from syncer import sync

from yaml_snippets.data_models import (
    MirrorData,
    MainConfig,
)
from yaml_snippets.utils import (
    config_validation,
    process_main_config,
    process_mirror_config,
    mirror_available,
)

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s',
)


class YamlFileType(argparse.FileType):

    def __call__(self, string):
        file_stream = super(YamlFileType, self).__call__(string)
        try:
            return {
                'config_path': file_stream.name,
                'config_data': yaml.safe_load(file_stream),
            }
        except yaml.YAMLError as err:
            raise argparse.ArgumentTypeError(
                f'The YAML file by path "{file_stream.name}" '
                f'is invalid because "{err}"'
            )


class JsonFileType(argparse.FileType):

    def __call__(self, string):
        file_stream = super(JsonFileType, self).__call__(string)
        try:
            return json.load(file_stream)
        except json.JSONDecodeError as err:
            raise argparse.ArgumentTypeError(
                f'The JSON file by path "{file_stream.name}" '
                f'is invalid because "{err}"'
            )


def create_parser():
    parser = argparse.ArgumentParser(
        description='The script checks validity of config (mirror or service) '
                    'schema. It checks a mirror config if you pass both '
                    'params, otherwise it checks only a service config.',
    )
    parser.add_argument(
        '-sc',
        '--service-config',
        default='config.yml',
        dest='service_config',
        help='Path to a service yaml config. Default path is ./config.yml.',
        required=True,
        type=YamlFileType('r'),
    )
    parser.add_argument(
        '-ss',
        '--service-config-json-schema',
        dest='service_config_json_schema',
        help='Path to a JSON schema of service config.',
        required=True,
        type=JsonFileType('r'),
    )
    parser.add_argument(
        '-mc',
        '--mirror-configs',
        dest='mirror_configs',
        help='A list of paths to a checked mirror config',
        default=[],
        nargs='+',
        type=YamlFileType('r'),
    )
    parser.add_argument(
        '-ms',
        '--mirror-config-json-schema',
        dest='mirror_config_json_schema',
        help='Path to a JSON schema of mirror config.',
        type=JsonFileType('r'),
    )
    return parser


async def are_mirrors_available(
        mirrors: list[MirrorData],
        main_config: MainConfig,
) -> int:
    ret_code = 0
    conn = TCPConnector(limit=10000, force_close=True)
    async with ClientSession(
            connector=conn,
            headers={"Connection": "close"}
    ) as http_session:
        for mirror in mirrors:
            mirror_name, is_available = await mirror_available(
                mirror_info=mirror,
                versions=main_config.versions,
                repos=main_config.repos,
                http_session=http_session,
                arches=main_config.arches,
                required_protocols=main_config.required_protocols,
                logger=logger,
            )
            if not is_available:
                ret_code = 1
    return ret_code


def do_mirrors_have_valid_geo_data(
        mirrors: list[MirrorData],
) -> int:
    ret_code = 0
    headers = {
        'referer': 'https://github.com/AlmaLinux/mirrors:CI'
    }
    url = 'https://nominatim.openstreetmap.org/search'
    ui_url = 'https://nominatim.openstreetmap.org/ui/details.html'
    for mirror in mirrors:
        if any(getattr(mirror.geolocation, geo_attr) is None for geo_attr in (
            'city', 'state', 'country'
        )):
            continue
        params = {
            'city': mirror.geolocation.city,
            'state': mirror.geolocation.state,
            'country': mirror.geolocation.country,
            'format': 'json',
        }
        req = requests.get(
            url=url,
            params=params,
            headers=headers,
        )
        try:
            req.raise_for_status()
            if req.json():
                logger.info(
                    'Mirror "%s" has valid geodata',
                    mirror.name,
                )
            else:
                logger.error(
                    'Mirror "%s" has invalid geodata. '
                    'Please check your data on "%s"',
                    mirror.name,
                    ui_url,
                )
                ret_code = 1
        except requests.RequestException as err:
            logger.warning(
                'Cannot check validity of mirror "%s" geodata because "%s"',
                mirror.name,
                err,
            )
    return ret_code


def main(args):
    is_validity, err = config_validation(
        yaml_data=args.service_config['config_data'],
        json_schema=args.service_config_json_schema,
    )
    if is_validity:
        logger.info(
            'Main config "%s" is valid',
            args.service_config['config_path'],
        )
    else:
        logger.error(
            'Main config "%s" is invalid because "%s"',
            args.service_config['config_path'],
            err,
        )
        exit(1)
    exit_code = 0
    for mirror_config in args.mirror_configs:
        is_validity, err = config_validation(
            yaml_data=args.service_config['config_data'],
            json_schema=args.service_config_json_schema,
        )
        if is_validity:
            logger.info(
                'The mirror config "%s" is valid',
                mirror_config['config_path'],
            )
        else:
            logger.error(
                'The mirror config "%s" is invalid because "%s"',
                mirror_config['config_path'],
                err,
            )
            exit_code = 1
    if not exit_code:
        logger.info('All configs are valid')
    mirrors = [
        process_mirror_config(
            yaml_data=mirror_config['config_data'],
            logger=logger,
        )
        for mirror_config in args.mirror_configs
    ]
    main_config, err_msg = process_main_config(
        yaml_data=args.service_config['config_data'],
    )
    exit_code += do_mirrors_have_valid_geo_data(
        mirrors=mirrors,
    )
    exit_code += sync(are_mirrors_available(
        mirrors=mirrors,
        main_config=main_config,
    ))
    exit(exit_code)


if __name__ == '__main__':
    arguments = create_parser().parse_args()
    main(arguments)
