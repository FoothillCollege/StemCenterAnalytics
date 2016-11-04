#!/usr/bin/env bash

# todo: ask bita on completing this + not using the git gui AT ALL, & copying to local automatically

# NOTE: sure however you automate deployment it supports the ability to extract email password/user
# name from dev_settings.py IN the heroku master repo and NOT included in the public repo
# also, make sure that other settings such as paths and what not are imported for use here
git add -f dev_settings.py  # forcefully add -- if not present it's ignored... (as in public push)
