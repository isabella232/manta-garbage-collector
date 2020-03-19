#!/bin/bash
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright 2020 Joyent, Inc.
#

printf '==> setup.sh @ %s\n' "$(date -u +%FT%TZ)"

set -o xtrace

NAME=manta-garbage-collector

#
# Once each time we (re)provision the "garbage-collector" zone this script will
# run and perform the setup functions.
#

SPOOL_DIR="/var/spool/manta_gc"
SVC_ROOT="/opt/smartdc/$NAME"

#
# Build PATH from this list of directories.  This PATH will be used both in the
# execution of this script, as well as in the root user .bashrc file.
#
paths=(
    "$SVC_ROOT/bin"
    "/opt/local/bin"
    "/opt/local/sbin"
    "/usr/sbin"
    "/usr/bin"
    "/sbin"
)

PATH=
for (( i = 0; i < ${#paths[@]}; i++ )); do
    if (( i > 0 )); then
        PATH+=':'
    fi
    PATH+="${paths[$i]}"
done
export PATH

if ! source "$SVC_ROOT/scripts/util.sh" ||
    ! source "$SVC_ROOT/scripts/services.sh"; then
        exit 1
fi

manta_common_presetup

manta_add_manifest_dir "/opt/smartdc/$NAME"

manta_common2_setup 'garbage-collector'

#
# Override the PATH 'manta_update_env' from manta-scripts.
#
# We don't want ".../node_modules/.bin" or the obsolete
# "/opt/smartdc/configurator/bin" on the PATH.
#
echo "" >>/root/.bashrc
echo "# Override PATH set by manta-scripts." >>/root/.bashrc
echo "export PATH=$PATH" >>/root/.bashrc

#
# Setup the delegated dataset for storing the garbage-collector instruction
# queue. This way, if we get reprovisioned while instructions are sitting
# around, they're not lost.
#
mkdir -p $SPOOL_DIR
dataset=zones/$(zonename)/data
if zfs list | grep $dataset; then
    mountpoint=$(zfs get -Hp mountpoint $dataset | awk '{print $3}')
    if [[ $mountpoint != $SPOOL_DIR ]]; then
        zfs set mountpoint=$SPOOL_DIR $dataset
        [[ $? -eq 0 ]] || fatal 'failed to set ZFS mountpoint'
    fi
else
    fatal 'must have delegated dataset so we do not lose instructions'
fi


#
# Import the garbage-collector SMF services. The manifest files create the
# services enabled by default.
#
for svc in \
    garbage-buckets-consumer \
    garbage-dir-consumer \
    garbage-uploader \
    ; do

    if ! svccfg import "/opt/smartdc/$NAME/smf/manifests/${svc}.xml"; then
        fatal "Could not import ${svc} SMF service."
    fi
done


#
# metricPorts are scraped by cmon-agent for prometheus metrics.
#
mdata-put metricPorts "8881,8882,8883"

manta_common2_setup_log_rotation "garbage-uploader"
manta_common2_setup_log_rotation "garbage-dir-consumer"
manta_common2_setup_log_rotation "garbage-buckets-consumer"

manta_common2_setup_end
