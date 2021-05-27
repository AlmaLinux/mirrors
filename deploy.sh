#!/bin/bash
# description: mirrors.almalinux.org content deployment tool.

set -e

# temporary hack to provide devel repos mirrorlist only for 8
pushd docs/.vuepress/public/mirrorlist/8/
sed 's/AppStream/devel/' appstream > devel
sed 's/AppStream/devel/' appstream-source > devel-source
sed 's/AppStream/devel/' appstream-debuginfo > devel-debuginfo
popd

npm run docs:build || yarn run docs:build || yarnpkg run docs:build

pushd docs/.vuepress/dist

git init
git add -A
git commit -m 'deploy'

git push -f git@github.com:AlmaLinux/mirrors.git master:gh-pages

popd
