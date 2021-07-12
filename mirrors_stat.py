#!/usr/bin/env python3

import argparse
import json
import os
import re
import subprocess
from collections import defaultdict
from json import JSONEncoder
from typing import (
    AnyStr,
    Union,
    List,
)
from dataclasses import (
    dataclass,
    field,
    is_dataclass,
    asdict,
)


HOURLY_METRIC_NAME = 'mirrors_stat_hourly'
DAILY_METRIC_NAME = 'mirrors_stat_daily'


class DataClassesJSONEncoder(JSONEncoder):
    """
    Custom JSON encoder for data classes
    """
    def default(self, o):
        if is_dataclass(o):
            return asdict(o)
        return super().default(o)


@dataclass
class Label:
    # name of a label
    name: AnyStr
    # value of a label
    value: AnyStr


@dataclass
class Metric:
    # name of a metric
    name: AnyStr
    # value of a metric
    value: Union[AnyStr, int]
    # labels of a metric
    labels: List[Label] = field(default_factory=list)


def save_statistics(
        stat_dir: AnyStr,
        stat_file: AnyStr,
        metrics: List[Metric],
) -> None:
    """
    Save the statistics to a stat file
    """

    if not os.path.exists(stat_dir):
        os.makedirs(stat_dir, exist_ok=True)
    stat_file_path = os.path.join(
        stat_dir,
        stat_file,
    )
    with open(stat_file_path, 'w') as f:
        for metric in metrics:
            if metric.labels:
                labels = metric.labels
                labels = (f'{label.name}={label.value}' for label in labels)
                labels = ','.join(labels)
            else:
                labels = ''
            metric_string = f'{metric.name}{{{labels}}} {metric.value}'
            f.write(f'{metric_string}\n')


def get_docker_logs(
        time_shift: AnyStr,
        container_name: AnyStr,
        docker_command: AnyStr,
) -> List[AnyStr]:
    """
    Get log lines from docker container
    """
    try:
        log_lines = subprocess.check_output(
            f'{docker_command} {container_name} --since {time_shift} 2>&1',
            shell=True,
            universal_newlines=True,
        )
    except subprocess.CalledProcessError:
        log_lines = []
    return log_lines.strip().split('\n')


def get_statistics(
        metric_name: AnyStr,
        time_shift: AnyStr,
        container_name: AnyStr,
        docker_command: AnyStr,
) -> List[Metric]:

    regexp = re.compile(
        r'(?P<address>(?:[0-9]{1,3}\.){3}[0-9]{1,3}).*GET '
        r'((/mirrorlist/(?P<dnf_version>\d\.?-?\w*)/'
        r'(?P<dnf_variant>\w*))|(/isos/(?P<isos_arch>\w*)/'
        r'(?P<isos_version>\d\.?-?\w*)))'
    )
    log_lines = get_docker_logs(
        time_shift=time_shift,
        container_name=container_name,
        docker_command=docker_command,
    )
    isos_metrics_dict = defaultdict(list)
    dnf_metrics_dict = defaultdict(list)
    for log_line in log_lines:
        match = regexp.search(log_line)
        if match is None:
            continue
        groups = match.groupdict()
        isos_version = groups['isos_version']
        isos_arch = groups['isos_arch']
        dnf_version = groups['dnf_version']
        dnf_variant = groups['dnf_variant']
        if isos_version and isos_arch:
            isos_metrics_dict[(
                isos_version.lower(),
                isos_arch.lower(),
            )].append(groups['address'])
        elif dnf_variant and dnf_version:
            dnf_metrics_dict[(
                dnf_version.lower(),
                dnf_variant.lower(),
            )].append(groups['address'])
    metrics = []
    for isos_metric_key, isos_metric_value in isos_metrics_dict.items():
        metrics.append(Metric(
            name=metric_name,
            value=len(set(isos_metric_value)),
            labels=[
                Label(
                    name='type',
                    value='isos',
                ),
                Label(
                    name='version',
                    value=isos_metric_key[0],
                ),
                Label(
                    name='arch',
                    value=isos_metric_key[1],
                ),
            ]
        ))
    for dnf_metric_key, dnf_metric_value in dnf_metrics_dict.items():
        metrics.append(Metric(
            name=metric_name,
            value=len(set(dnf_metric_value)),
            labels=[
                Label(
                    name='type',
                    value='dnf',
                ),
                Label(
                    name='version',
                    value=dnf_metric_key[0],
                ),
                Label(
                    name='variant',
                    value=dnf_metric_key[1],
                ),
            ]
        ))
    return metrics


def create_parser():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        '--docker-command',
        help='Docker command for viewing logs of a container',
        action='store',
        default='docker logs'
    )
    parser.add_argument(
        '--container-name',
        help='Name of docker container for getting statistics',
        action='store',
        default='alma-mirrors'
    )
    stat_period = parser.add_mutually_exclusive_group(required=True)
    stat_period.add_argument(
        '--hourly-stat',
        help='Get and save the statistics for last one hour',
        action='store_true',
        default=False,
    )
    stat_period.add_argument(
        '--daily-stat',
        help='Get and save the statistics for last 24 hours',
        action='store_true',
        default=False,
    )
    stat_period.add_argument(
        '--custom-period',
        help='Get and save the statistics for custom period',
        action='store',
        default=False,
    )
    stat_options = parser.add_mutually_exclusive_group(required=True)
    stat_options.add_argument(
        '--dry-run',
        help='Print the statistics as json instead saving it to stat files',
        action='store_true',
        default=None,
    )
    stat_options.add_argument(
        '--stat-dir',
        help='Path to a directory with the stat files for Node Exporter',
        action='store',
        default='/var/run/node_exporter'
    )
    return parser


def cli_main():
    args = create_parser().parse_args()
    if args.custom_period:
        metric_name = 'mirrors_stat_custom_period'
        time_shift = args.custom_period
    elif args.daily_stat:
        metric_name = DAILY_METRIC_NAME
        time_shift = '24h'
    else:
        metric_name = HOURLY_METRIC_NAME
        time_shift = '1h'
    metrics = get_statistics(
        metric_name=metric_name,
        time_shift=time_shift,
        container_name=args.container_name,
        docker_command=args.docker_command,
    )
    if args.dry_run is None:
        save_statistics(
            stat_dir=args.stat_dir,
            stat_file=f'{metric_name}.prom',
            metrics=metrics,
        )
    else:
        stats_in_json = json.dumps(
            metrics,
            indent=4,
            cls=DataClassesJSONEncoder,
        )
        print(stats_in_json)


if __name__ == '__main__':
    cli_main()
