#!/usr/bin/env python3

import yaml
import os
import sys

# read config
with open('config.yml') as f:
    config = yaml.safe_load(f)
    mirrorlist_dir = config['mirrorlist_dir']
    mirrors_dir = config['mirrors_dir']

# read and verify mirrors
all_mirrors = []
for mirror_file in os.listdir(mirrors_dir):
    with open(mirrors_dir + '/' + mirror_file) as f:
        # filter broken or unavailable mirrors
        try:
            mirror = yaml.safe_load(f)
            mirror['address']
            mirror['name']
        except:
            print('Cannot load mirror data from file ' + mirror_file)
            continue
        all_mirrors.append(mirror)

# exit if no mirrors found
if all_mirrors == []:
    sys.exit('No mirrors found')

# remove md table file if exists
if os.path.exists(config['mirrors_table']): os.remove(config['mirrors_table'])
# genetate md table
with open(config['mirrors_table'], 'a') as f:
    print('| Name | Sponsor | HTTP | HTTPS | RSYNC |\n| --- | --- | --- | --- | --- |', file=f)
    for mirror in all_mirrors:
        for protocol in ['http', 'https']:
            try:
                mirror[protocol + '_link'] = '[Mirror](' + mirror['address'][protocol] + ')'
            except:
                mirror[protocol + '_link'] = ''
        for protocol in ['rsync']:
            try:
                mirror[protocol + '_link'] = '[Link](' + mirror['address'][protocol] + ')'
            except:
                mirror[protocol + '_link'] = ''
        # print('|' + mirror['name'] + '| [' + mirror['sponsor'] + '](' + mirror['sponsor_url'] + ')|' + mirror['https_link'] + '|' + mirror['http_link'] + '|' + mirror['rsync_link'] + '|', file=f)
        print("|%s|[%s](%s)|%s|%s|%s|" % (mirror['name'],mirror['sponsor'],mirror['sponsor_url'],mirror['http_link'],mirror['https_link'],mirror['rsync_link']), file=f)
