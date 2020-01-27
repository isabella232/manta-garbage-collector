#!/bin/bash
#
# Copyright 2020 Joyent, Inc.
#
# This attempts to find the async peer for each manatee shard in this DC.
#

thisDC=$(sysinfo | json "Datacenter Name")
echo "# Datacenter is '$thisDC'"

zkServers=$(sdc-sapi /applications?name=manta | json -H 0.metadata.ZK_SERVERS | json -a host port | tr ' ' ':' | xargs | tr ' ' ',')
echo "# Zookeepers: [$zkServers]"

# manta-oneach only works for local zones, so we have to find the nameservice
# zone in *this* DC.
zkInstJson=$(manta-oneach -J -s nameservice "zonename" | head -1)
zkInstExit=$(json result.exit_status <<<$zkInstJson)
if [[ $zkInstExit -ne 0 ]]; then
    echo "Failed to find working ZK instance in this DC" 2>&1
    exit 1
fi

zkInst=$(json result.stdout <<<$zkInstJson | head -1)
echo "# Using ZK $zkInst"

target_zones=
shards=0
for shard in $(sdc-sapi /applications?name=manta | json -H 0.metadata.INDEX_MORAY_SHARDS | json -Ha host); do
    shards=$((shards + 1))
    state=$(manta-oneach -J -z ${zkInst} "zkCli.sh get /manatee/${shard}/state")
    if [[ $(json result.exit_status <<<$state) -eq 0 ]]; then
        stateObj=$(json result.stdout <<<$state | grep '"generation":')
        async=$(json async.0.zoneId <<<$stateObj)
        echo "# $stateObj"
        if [[ -n $async ]]; then
            asyncDC=$(sdc-sapi /instances/${async} | json -H metadata.DATACENTER)
            if [[ $asyncDC == $thisDC ]]; then
                if [[ -n $target_zones ]]; then
                    target_zones="${target_zones},"
                fi
                echo "SHARD: $shard ASYNC: $async"
            else
                echo "WARNING: Async ($async) is in DC '$asyncDC', skipping"
            fi
        else
            echo "WARNING: Shard $shard has no async, skipping."
        fi
    else
        echo "FAILED: $(json result.exit_status <<<$state)"
    fi
done
