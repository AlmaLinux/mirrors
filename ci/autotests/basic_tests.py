import unittest
import yaml
import requests
import logging
import ddt
import os


@ddt.ddt
class TheMirrorsService(unittest.TestCase):

    service_url = os.environ.get(
        'al_mirrors_service_url',
        'http://mirrors-dev.almalinux.org',
    )
    config = None
    config_url = (
        'https://raw.githubusercontent.com'
        '/AlmaLinux/mirrors/master/config.yml'
    )
    versions = None
    arches = None
    vault_versions = None
    duplicated_versions = None
    repos = None
    vault_mirror = None

    @classmethod
    def _load_service_config(cls):
        response = requests.get(cls.config_url)
        response.raise_for_status()
        content = response.text
        cls.config = yaml.safe_load(content)
        cls.versions = cls.config['versions']
        cls.vault_versions = cls.config['vault_versions']
        cls.duplicated_versions = cls.config['duplicated_versions']
        cls.repos = cls.config['repos']
        cls.vault_mirror = cls.config['vault_mirror']
        cls.arches = cls.config['arches']

    @staticmethod
    def _parse_mirror_list(content: str) -> list[str]:
        return content.split('\n')

    def _make_mirror_suffix(self, version: str, repo_path: str) -> str:
        if version in self.duplicated_versions:
            version = self.duplicated_versions[version]
        return f'{version}/{repo_path}'

    def _make_isos_mirror_suffix(self, version: str, arch: str) -> str:
        if version in self.duplicated_versions:
            version = self.duplicated_versions[version]
        return f'{version}/isos/{arch}'

    def test_01_versions_and_repos(self):
        for repo_dict in self.repos:
            repo_name = repo_dict['name']
            repo_path = repo_dict['path']
            repo_versions = repo_dict.get('versions', self.versions)
            if repo_dict.get('vault', False):
                continue
            logging.info('Check repo "%s"', repo_name)
            for version in self.versions:
                logging.info('Check version "%s"', version)
                if version not in repo_versions:
                    continue
                url = f'{self.service_url}/mirrorlist/{version}/{repo_name}'
                response = requests.get(url)
                response.raise_for_status()
                mirrors_list = self._parse_mirror_list(response.text)
                for mirror in mirrors_list:
                    mirror_suffix = self._make_mirror_suffix(
                        version,
                        repo_path,
                    )
                    msg = (
                        f'Wrong mirror "{mirror}" '
                        f'for mirror_suffix "{mirror_suffix}'
                    )
                    self.assertTrue(mirror.endswith(mirror_suffix), msg=msg)

    @ddt.unpack
    @ddt.data(
        (
                '13.67.153.16',
                'http://eastus.azure.repo.almalinux.org/almalinux/',
        ),
        (
                '13.104.156.0',
                'http://germanywestcentral.azure.repo.almalinux.org/almalinux/',
        ),
        (
                '13.105.99.64',
                'http://southeastasia.azure.repo.almalinux.org/almalinux/',
        ),
        (
                '13.67.128.0',
                'http://westus2.azure.repo.almalinux.org/almalinux/',
        ),
        (
                '77.79.198.14',
                'http://centos.corp.cloudlinux.com/almalinux/',
        ),
        (
                '77.121.201.30',
                'http://mirror.vsys.host/almalinux/',
        ),
        (
                '77.121.201.30',
                'http://almalinux.netforce.hosting/almalinux/',
        ),
    )
    def test_02_specific_ips(self, ip, mirror):
        logging.info('Check IP: "%s" for mirror "%s"', ip, mirror)
        headers = {
            'X-Forwarded-For': ip,
        }
        version = self.versions[len(self.versions) - 1]
        repo_dict = self.repos[0]
        repo_name = repo_dict['name']
        repo_path = repo_dict['path']
        mirror_suffix = self._make_mirror_suffix(version, repo_path)
        mirror = f'{mirror}{mirror_suffix}'
        url = f'{self.service_url}/mirrorlist/{version}/{repo_name}'
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        mirrors_list = self._parse_mirror_list(response.text)
        self.assertTrue(
            mirror in mirrors_list,
            msg=f'{mirror} is absent in {mirrors_list}',
        )

    def test_03_vault_repos(self):
        for repo_dict in self.repos:
            repo_name = repo_dict['name']
            repo_path = repo_dict['path']
            repo_versions = repo_dict.get('versions', self.versions)
            if not repo_dict.get('vault', False):
                continue
            logging.info('Check vault repo "%s"', repo_name)
            for version in repo_versions:
                logging.info('Check version "%s"', version)
                url = f'{self.service_url}/mirrorlist/{version}/{repo_name}'
                response = requests.get(url)
                response.raise_for_status()
                mirrors_list = self._parse_mirror_list(response.text)
                mirror_suffix = self._make_mirror_suffix(
                    version,
                    repo_path,
                )
                actual_mirrors_list = [
                    f'{self.vault_mirror}{mirror_suffix}'
                ]
                self.assertListEqual(
                    mirrors_list,
                    actual_mirrors_list,
                )

    def test_04_vault_versions(self):
        repo_dict = self.repos[0]
        repo_name = repo_dict['name']
        repo_path = repo_dict['path']
        for version in self.vault_versions:
            logging.info('Check vault version "%s"', version)
            mirror_suffix = self._make_mirror_suffix(version, repo_path)
            mirror = f'{self.vault_mirror}{mirror_suffix}'
            url = f'{self.service_url}/mirrorlist/{version}/{repo_name}'
            response = requests.get(url)
            response.raise_for_status()
            mirrors_list = self._parse_mirror_list(response.text)
            self.assertListEqual(mirrors_list, [mirror])

    def test_05_isos_list(self):
        for version in self.versions:
            logging.info('Check version "%s"', version)
            for arch in self.arches:
                logging.info('Check architecture "%s"', arch)
                url = f'{self.service_url}/isolist/{version}/{arch}'
                response = requests.get(url)
                response.raise_for_status()
                mirrors_list = self._parse_mirror_list(response.text)
                for mirror in mirrors_list:
                    mirror_suffix = self._make_isos_mirror_suffix(
                        version,
                        arch,
                    )
                    msg = (
                        f'Wrong ISOs mirror "{mirror}" '
                        f'for mirror_suffix "{mirror_suffix}'
                    )
                    self.assertTrue(mirror.endswith(mirror_suffix), msg=msg)

    @classmethod
    def setUpClass(cls) -> None:
        logging.basicConfig(level=logging.INFO)
        cls._load_service_config()
        logging.info('Check the service by url "%s"', cls.service_url)


if __name__ == '__main__':
    unittest.main()
