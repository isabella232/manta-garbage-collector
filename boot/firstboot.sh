#!/bin/bash
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright 2019 Joyent, Inc.
#

printf '==> firstboot @ %s\n' "$(date -u +%FT%TZ)"

set -o xtrace

NAME=manta-garbage-collector

#
# Runs on first boot of a newly reprovisioned "garbage-collector" zone.
# (Installed as "setup.sh" to be executed by the "user-script")
#

SPOOL_DIR="/var/spool/manta_gc"
SVC_ROOT="/opt/smartdc/$NAME"
SAPI_CONFIG="$SVC_ROOT/etc/config.json"

#
# Build PATH from this list of directories.  This PATH will be used both in the
# execution of this script, as well as in the root user .bashrc file.
#
paths=(
	"$SVC_ROOT/bin"
	"$SVC_ROOT/node_modules/.bin"
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


#
# This exists to help us move past the MANTA-4251 flag-day change where we need
# to use the GC_INSTR_WRITE_* variables in the metadata instead of
# GC_INSTR_UPLOAD_*. If it determines the config needs to be updated, it will
# copy the old values to the new, leaving the old values in place in order to
# support rollback.
#
# Once every existing garbage-collector zone has been updated past this
# flag-day, this code can be removed.
#
function upgrade_sapi_config {
    local new_json

    SAPI_SERVICE_JSON=$(curl -sS $SAPI_URL/services?name=garbage-collector | json -H 0)
    if [[ -z $SAPI_SERVICE_JSON ]]; then
        echo "WARN: Unable to read SAPI service, skipping upgrade attempt."
        return
    fi
    SAPI_SERVICE_UUID=$(json uuid <<<$SAPI_SERVICE_JSON)
    [[ -n $SAPI_SERVICE_UUID ]] || fatal "Unable to determine SAPI server UUID."

    #
    # If GC_INSTR_WRITE_BATCH_SIZE is set, we assume the upgrade already
    # happened since *something* has updated the config to know about this
    # new parameter.
    #
    if [[ -n $(json metadata.GC_INSTR_WRITE_BATCH_SIZE <<<$SAPI_SERVICE_JSON) ]]; then
        echo "Config already contains GC_INSTR_WRITE_BATCH_SIZE, skipping upgrade."
        return;
    fi

    echo "Attempting to upgrade config."

    GC_INSTR_UPLOAD_BATCH_SIZE=$(json metadata.GC_INSTR_UPLOAD_BATCH_SIZE <<<$SAPI_SERVICE_JSON)
    GC_INSTR_UPLOAD_FLUSH_DELAY=$(json metadata.GC_INSTR_UPLOAD_FLUSH_DELAY <<<$SAPI_SERVICE_JSON)
    if [[ -z $GC_INSTR_UPLOAD_BATCH_SIZE || -z $GC_INSTR_UPLOAD_FLUSH_DELAY ]]; then
        #
        # This is fatal, because if these are missing, we're also not going to
        # be able to start the service since the config-agent generated config
        # will be broken JSON.
        #
        fatal "Both GC_INSTR_UPLOAD_BATCH_SIZE and GC_INSTR_UPLOAD_FLUSH_DELAY must be set in order to upgrade config."
    fi

    GC_INSTR_UPLOAD_MIN_BATCH_SIZE=$(json metadata.GC_INSTR_UPLOAD_MIN_BATCH_SIZE <<<$SAPI_SERVICE_JSON)
    # Use default from MANTA-4249 if MIN_BATCH_SIZE unset
    [[ -z $GC_INSTR_UPLOAD_MIN_BATCH_SIZE ]] && GC_INSTR_UPLOAD_MIN_BATCH_SIZE=1

    new_json='{"action": "update", "metadata": {}}'
    new_json=$(json -e "this.metadata.GC_INSTR_WRITE_MIN_BATCH_SIZE=$GC_INSTR_UPLOAD_MIN_BATCH_SIZE" <<<$new_json)

    # Just to make sure, we'll also not overwrite the other keys (we checked
    # GC_INSTR_WRITE_BATCH_SIZE already).
    if [[ -z $(json metadata.GC_INSTR_WRITE_BATCH_SIZE <<<$SAPI_SERVICE_JSON) ]]; then
        new_json=$(json -e "this.metadata.GC_INSTR_WRITE_BATCH_SIZE=$GC_INSTR_UPLOAD_BATCH_SIZE" <<<$new_json)
    fi
    if [[ -z $(json metadata.GC_INSTR_WRITE_FLUSH_DELAY <<<$SAPI_SERVICE_JSON) ]]; then
        new_json=$(json -e "this.metadata.GC_INSTR_WRITE_FLUSH_DELAY=$GC_INSTR_UPLOAD_FLUSH_DELAY" <<<$new_json)
    fi

    echo "Updating SAPI with:"
    echo "$new_json"

    curl -sS -X PUT -d @- $SAPI_URL/services/$SAPI_SERVICE_UUID <<<$new_json
    curl_ret=$?
    [[ $curl_ret -eq 0 ]] || fatal "PUT failed for SAPI service, exit code: $curl_ret"
}


manta_common_presetup

upgrade_sapi_config

manta_add_manifest_dir "/opt/smartdc/$NAME"

manta_common_setup 'garbage-collector'

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
# Import the garbage-collector SMF service.  The manifest file creates the service
# enabled by default.
#
if ! svccfg import "/opt/smartdc/$NAME/smf/manifests/garbage-collector.xml"; then
	fatal 'could not import garbage-collector SMF service'
fi

#
# Import the rsyncd SMF service.  The manifest file creates the service
# enabled by default.
#
if ! svccfg import "/opt/smartdc/$NAME/smf/manifests/rsyncd.xml"; then
	fatal 'could not import rsyncd SMF service'
fi

#
# Get the port from the sapi config file, then add 1000 to it to get the
# metricPort to allow scraping by cmon-agent.
#
# Note that the config file is guaranteed to have been written by config-agent
# at this point, because we've already called manta_common_setup, which calls
# manta_enable_config_agent, which enables the config-agent service
# synchronously.
#
if [[ ! -f $SAPI_CONFIG ]]; then
	fatal 'SAPI config not found'
fi
mdata-put metricPorts $(expr $(< "$SAPI_CONFIG" json 'port') + 1000)

manta_common_setup_end
