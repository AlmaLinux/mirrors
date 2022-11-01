# coding=utf-8

import requests
import logging
import os
from typing import Optional

import sentry_sdk
from sentry_sdk.integrations.flask import FlaskIntegration
from sentry_sdk.integrations.aiohttp import AioHttpIntegration


def get_deploy_environment_name() -> str:
    """
    Get deploy environment name from the file
    """

    with open(os.getenv('DEPLOY_ENVIRONMENT'), 'r') as fd:
        deploy_env_name = fd.read().strip()
    return deploy_env_name


def get_aws_instance_api() -> str:
    """
    Get IP of a current AWS instance
    """

    meta_data_url = 'http://169.254.169.254/latest/meta-data/public-ipv4'
    try:
        req = requests.get(url=meta_data_url)
        req.raise_for_status()
        return req.text
    except (requests.ConnectionError, requests.RequestException):
        return 'ItIsNotAWSInstance'


def init_sentry_client(dsn: Optional[str] = None) -> None:
    """
    Initialize sentry client with default options
    :param dsn: project auth key
    """
    deploy_env_name = get_deploy_environment_name()
    if dsn is None and os.getenv('SENTRY_DSN') is None:
        logging.warning('Sentry DSN is not defined')
    if os.getenv('SENTRY_DISABLED') == 'True':
        logging.warning('Sentry is disabled')
        return

    if dsn is None:
        dsn = os.getenv('SENTRY_DSN')
    # sentry performance monitoring
    if deploy_env_name == 'Production':
        traces_sample_rate = 0.01
    else:
        traces_sample_rate = 1.0
    sentry_sdk.init(
        dsn=dsn,
        environment=deploy_env_name,
        ignore_errors=[
            KeyboardInterrupt,
        ],
        integrations=[
            FlaskIntegration(),
            AioHttpIntegration()

        ],

        traces_sample_rate=traces_sample_rate
    )
    if not os.getenv('SKIP_AWS_CHECKING'):
        with sentry_sdk.configure_scope() as scope:
            scope.set_tag('aws_instance_ip', get_aws_instance_api())


def get_logger(logger_name: str):
    """
    Create or get existing logger
    :param logger_name: Name of the new or existing logger
    :return: logging object
    """

    # create logger or get existing
    logger = logging.Logger(logger_name)
    # Set handler if it doesn't exist
    if not len(logger.handlers):
        deploy_environment = get_deploy_environment_name()
        if deploy_environment.lower() in ('production', 'staging'):
            logging_level = logging.WARNING
        else:
            logging_level = logging.INFO
        logger.setLevel(logging_level)
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging_level)
        formatter = logging.Formatter(
            '%(asctime)s | %(name)s | %(levelname)s | %(message)s'
        )
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)
    return logger
