# Quick overview

* [Requirements](#Requirements)
* [How to deploy](#Deploying)
* [How it works](#How)


## Requirements

* CentOS 8 (target server)
* Ansible version is 2.10 or newer
* Ansible community.docker collection
    * Use command `ansible-galaxy collection install community.docker` for installing it

## Deploying

1. Clone GitHub repo `https://github.com/AlmaLinux/mirrors.git`
2. Switch to branch `mirrors_service`
3. Go to project directory `ci/ansible/inventory`
4. Copy template inventory `template` to a new file
5. Fill the following items
    1. In section `mirrors_service` place an IP of server for deploying
    2. Set value of option `container_env_prefix` to `prod` in case of production layout or `dev` in other cases.
    3. Set value of option `sentry_dsn` to your DSN key of a Sentry. 
       Skip this step if you don't have this key
    4. Set value of option `deploy_environment` to `Production` if you deploy 
       production variant of the service. Otherwise, use `Dev` or something like this.
    5. Set value of option `auth_key` to random string. E.g. `124120bydf978g6sd9fg`
    6. Set value of option `backend_port` to value of port which will be used 
       for proxying requests from nginx to container. The container will 
       accept only local connections if you set value of `deploy_environment` to `Production`. 
       Otherwise, the container will accept remote connections. 
    7. Set value of option `test_ip_address` to test IP. This options is work only for non-production layout.
    8. Set value of options `uwsgi_address` and `uwsgi_port`. Those are used for starting uwsgi service inside a container.
6. Go to project directory `ci/ansible`
6. Run command `ansible-playbook -vv -i inventory/dev -u 
   <username> --become main.yml`, there is `<username>` is name of a user from a remote server which has sudo rights

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