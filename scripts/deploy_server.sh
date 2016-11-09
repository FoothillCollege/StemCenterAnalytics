#!/usr/bin/env bash
# forcefully add private settings to server deployment (overrides gitignore)

if [ $# -ne 2 ] ; then
   echo "Usage: deploy_server.sh title description" ; exit 0
fi

title=$1
description=$2

cd ..

git add . -f dev_settings.py
git commit -m ${title} -m ${description}
git push heroku master
