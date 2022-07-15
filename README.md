# Quick overview

* [Requirements](#Requirements)
* [How to deploy](#Deploying)
* [How it works](#How-it-works)
* [Statistics of the mirrors service](#Statistics)

## Requirements

* CentOS 8 (target server)
* Ansible version is 2.10 or newer - make sure you have the full version installed with: `sudo dnf install -y epel-release && sudo dnf install -y ansible`
* Ansible community.docker collection
    * Use command `ansible-galaxy collection install community.docker` for installing it
* Ansible ssh-reconnect role
    * Use command `ansible-galaxy install udondan.ssh-reconnect` for installing it

## Deploying

1. Clone GitHub repo `https://github.com/AlmaLinux/mirrors.git`
2. Switch to branch `mirrors_service`
3. Remember to clone git submodules too: `git submodule init && git submodule update --remote`
4. Go to project directory `ci/ansible/inventory`
5. Copy template inventory `template` to a new file
6. Fill the following items
    1. In section `mirrors_service` place an IP of server for deploying
    2. Set value of option `container_env_prefix` to `prod` in case of production layout or `dev` in other cases.
    3. Set value of option `sentry_dsn` to your DSN key of a Sentry. 
       Skip this step if you don't have this key
    4. Set value of option `deploy_environment` to `Production` if you deploy 
       production variant of the service. Otherwise, use `Dev` or something like this.
    5. Set value of option `auth_key` to random string. E.g. `124120bydf978g6sd9fg`
    7. Set value of option `test_ip_address` to test IP. This options is work only for non-production layout.
    8. Set value of option `gunicorn_port`. That is used for starting backend service.
7. Go to project directory `ci/ansible`
8. Run command `ansible-playbook -vv -i inventory/vagrant -u vagrant --become main.yml`, where:  
   `vagrant` with `-u` is name of a user from a remote server which has sudo rights
   and `inventory/vagrant` is the *new file* mentioned earlier

## How it works
The service uses IP of a incoming request for detecting country and region. Therefore, a location can be different by expected if a server or you are using proxy, vpn, Cloudflare (or something like this).
For detecting of location by IP we use GeoIP database from https://www.maxmind.com/en/geoip2-databases.
The service returns full list of mirrors if it can't detect your location by IP.
Otherwise, it returns list of the ten nearest mirrors. The list make up by following methods:
- Take the ten nearest to you mirrors inside your country (e.g. inside UK)
- Take the ten nearest to you mirrors inside your region (e.g. Europe). This list doesn't include the previous list.
- Take the ten nearest to you mirrors outside your region and country. This list doesn't include the previous lists too.
- Length of each list can be less than ten, because your country/region may not contain required amount of mirrors.
- Each next mirrors list adds to previous
- The final list is trimmed to first ten elements and it's returned to a client.
  
Example:
- Your location is Egypt.
- Egypt contains four mirrors.
- Africa contains five mirrors.
- Other world contains 95 mirrors.
- The service takes four mirrors from Egypt, five mirrors from Africa and ten mirrors from other world.
- It trims this list to ten elements and return it to client:
- | E | E | E | E | A | A | A | A | A | W |, there is E - Egypt mirror, A - Africa mirror, W - World mirror

## Statistics

Script `mirrors_stat.py` can collect statistics of using the mirrors service for following url:
* `/mirrorlist/<version>/<repo>`
* `/isos/<arch>/<version>`

The script puts this statistics to Node Exporter stats files or can print it to stdout.

Use help of the script for more information.

## Forking

The project uses the file `./ci/ansible/roles/deploy/tasks/get_mirrors_repo.yml` as a reference, which repository to clone on the target machine that has the mirror list.

Fork that repository, add your own mirrors (just provide a proper yaml in the `mirrors.d` directory in that repo), change the reference repo in the file mentioned above and you should be good to go. Just make sure your mirror follows the same structure as AlmaLinux does, so it has e.g. the following path: `[...]/8.6/BaseOS/x86_64/os/repodata/repomd.xml`.

Remember to clone git submodules too: `git submodule init && git submodule update --remote`

Remember to change all git submodules' references to point to your new fork.

Make sure you have a valid MaxMind license key - provide it in the file ./ci/ansible/inventory/vagrant with the `license_key` variable.

You can disable Sentry completely in the file ./ci/ansible/roles/deploy/templates - just set `SENTRY_DISABLED=True`.

### Deploy a fake mirror for testing

Fork the project as suggested above and use the following file as the only `.yml` file in the `mirrors.d` directory (the one on the `master` branch):

```
---
name: 10.0.0.11
address:
  https: http://10.0.0.11/almalinux
geolocation:
  country: test
  city: test
update_frequency: 3h
sponsor: test
sponsor_url: https://10.0.0.11
email: vagrant@10.0.0.11
...
```

Then `vagrant up fake-mirror && vagrant ssh fake-mirror` and use the following commands to make a stub of a real mirror:

```
sudo dnf install -y createrepo python3
mkdir almalinux && cd almalinux
mkdir -p {8.6,9.0}/{BaseOS,AppStream,PowerTools,HighAvailability,CRB,NFV,RT,SAP,SAPHANA,ResilientStorage,cloud,devel,extras,isos,live,metadata,plus,raspberrypi}/{x86_64,aarch64,ppc64le,s390x}/os
for i in {8.6,9.0}/{BaseOS,AppStream,PowerTools,HighAvailability,CRB,NFV,RT,SAP,SAPHANA,ResilientStorage,cloud,devel,extras,isos,live,metadata,plus,raspberrypi}/{x86_64,aarch64,ppc64le,s390x}/os ; do cd $i ; createrepo_c . ; cd ../../../.. ; done
date +%s > TIME
cd
nohup sudo python3 -m http.server 80 &
```

You should now be ready to test the mirrors-service machine.

### Test it

```
vagrant up mirrors-service
cd ci/ansible
ansible-playbook -vv -i inventory/vagrant -u vagrant --become main.yml
```

After the tasks have finishes, wait about 10 minutes for the repositories to load.

## Troubleshooting

Run the command `sudo systemctl status mirror_service_backend.service` inside the Vagrant machine and hope the logs provide some hints on what went wrong. Good luck! :)
