#!/usr/bin/env bash
# unlike deploy_server.sh, different repo without private settings included
if [ $# -ne 2 ] ; then
   echo "Usage: deploy_server.sh title description" ; exit 0
fi

title=$1
description=$2

cd ..

git commit -m ${title} -m ${description}
git push StemCenterAnalytics master
