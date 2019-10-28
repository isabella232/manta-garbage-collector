#!/bin/bash


if [[ $# != 2 ]];
then
	echo "usage: ./assign_shards_to_collectors.sh NSHARDS NCOLLECTORS"
	exit 1
fi

NSHARDS=$1
NCOLLECTORS=$2

re=/^[0-9]+$/

if [[ $NSHARDS =~ $re ]];
then
	echo "usage: invalid shard number: $NSHARDS"
	exit 1
fi

if [[ $NCOLLECTORS =~ $re ]];
then
	echo "usage: invalid collector count: $NCOLLECTORS"
	exit 1
fi

#
# This script rounds down. Any remainders left must be accounted for by
# the operator for now.
#
SHARDS_PER_COLLECTOR=$(( NSHARDS / NCOLLECTORS ))

#
# This script is specifically intended for production Manta deployments,
# for which the first index shard in the numerical ordering is called
# 2.moray.{{DOMAIN_NAME}}.
#
SHARD_NUM_ITER=1

echo "Searching for garbage-collector service in SAPI..."
GC_SERVICE_UUID=$(sdc-sapi /services?name=garbage-collector | \
	json -Ha uuid)
if [[ -z "$GC_SERVICE_UUID" || $? != 0 ]];
then
	echo "Aborting. Could not find garbage-collector service. Have "
	echo "you run manta-init from an updated manta-deployment zone?"
	exit 1
fi

echo "Searching for garbage-collector instances in SAPI..."
GC_INSTANCE_UUIDS=$(sdc-sapi /instances?service_uuid=${GC_SERVICE_UUID} | \
	json -Ha uuid)
if [[ -z "$GC_INSTANCE_UUIDS" || $? != 0 ]];
then
	echo "Aborting. Could not find any garbage-collector instances."
	eixt 1
fi

NFOUND=$(echo "$GC_INSTANCE_UUIDS" | wc -l | tr -d '[:space:]')
if [[ $NFOUND != $NCOLLECTORS ]];
then
	echo "Found $NFOUND collectors, expected $NCOLLECTORS."
	exit 1
fi

echo "Found garbage-collector instances:"
echo "${GC_INSTANCE_UUIDS}"

PROMPT="Assigning $SHARDS_PER_COLLECTOR shards to each of $NCOLLECTORS "
PROMPT+="garbage-collectors(s). OK? [Y/n]: "
read -p "$PROMPT" ok

if [[ "$ok" != "Y" ]];
then
	echo "Abort."
	exit 1
fi

for instance_uuid in ${GC_INSTANCE_UUIDS};
do
	update_file="/var/tmp/CM-2082-${instance_uuid}.json"

	LO=${SHARD_NUM_ITER}
	HI=$((SHARD_NUM_ITER + SHARDS_PER_COLLECTOR - 1))

	sapiadm get $instance_uuid | json -e "
		this.metadata.GC_SHARD_NUM_LO=$LO;
		this.metadata.GC_SHARD_NUM_HI=$HI
	" &> ${update_file}

	echo -n "Assigning shards [$LO-$HI] to ${instance_uuid}..."

	sapiadm update ${instance_uuid} -f ${update_file}

	if [[ $? == 0 ]];
	then
		echo "success."
	else
		echo "FAILED."
		echo "See update json: ${update_file}"
	fi

	SHARD_NUM_ITER=$(( HI + 1 ))
done;

echo "Done."
