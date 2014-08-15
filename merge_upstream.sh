#!/bin/bash

set -e
set -x

git fetch upstream
git checkout master
git merge upstream/master
