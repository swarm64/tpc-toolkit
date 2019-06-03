#!/bin/bash

version=$1
target=swarm64-tpc-toolkit-$version
target_dir=/tmp/${target}

rsync -av --exclude deploy.sh --exclude *pyc* --exclude .git* . ${target_dir}/

tar -czf ${target_dir}.tar.gz -C ${target_dir}/../ ${target}
