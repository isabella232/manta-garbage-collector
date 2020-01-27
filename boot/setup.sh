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
# (Installed as "setup.sh" which is the standard name, and this will be executed
# by the "user-script")
#

SPOOL_DIR="/var/spool/manta_gc"
SVC_ROOT="/opt/smartdc/$NAME"

#
# Build PATH from this list of directories.  This PATH will be used both in the
# execution of this script, as well as in the root user .bashrc file.
#
paths=(
    "$SVC_ROOT/bin"
    "$SVC_ROOT/node/bin"
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

manta_common_setup 'garbage-collector'

#
# XXX WTF
#
# Replace the contents of PATH from the default root user .bashrc with one
# more appropriate for this particular zone.
#
if ! /usr/bin/ed -s '/root/.bashrc'; then
    fatal 'could not modify .bashrc'
fi <<EDSCRIPT
/export PATH/d
a
export PATH="$PATH"
.
w
EDSCRIPT


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

manta_common_setup_end
