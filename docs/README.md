# Deploying the garbage-collector

The `garbage-collector` is the component which handles pulling records for
deleted objects from the metadata tier, and sending instructions to storage
zones to delete the unneeded objects.

## Get the Latest Image

To deploy garbage-collector you'll probably first want to get the latest
`garbage-collector` image. To do so you can run:

```
GARBAGE_IMAGE=$(updates-imgadm list name=mantav2-garbage-collector --latest -H -o uuid)
sdc-imgadm import -S https://updates.joyent.com ${GARBAGE_IMAGE}
```

if this returns an error that the image already exists, you already have the
latest image.

## Configuration: Option A

Once you've got the latest image installed in your DC, you can generate a config
using:

```
manta-adm gc genconfig ${GARBAGE_IMAGE} ${NUM_COLLECTORS} > gc-config.json
```

where `${NUM_COLLECTORS}` is the number of collectors you want to have. This
should be a number less than or equal to the number of shards you have and the
number of shards each garbage collector will need to collect from will be:

```
NUM_SHARDS / NUM_COLLECTORS
```

So if you have 50 shards and want each collector to collect for 10 shards,
you'd run:

```
manta-adm gc genconfig ${GARBAGE_IMAGE} 5 > gc-config.json
```

to generate a config with 5 `garbage-collector` zones.

If you get the error:

```
no CNs meet deployment criteria
```

this means you do not have enough CNs that:

 * do not have a `storage` zone
 * do not have a `nameservice` zone
 * do not have a `loadbalancer` zone

You need to have at least `${NUM_COLLECTORS}` nodes that match these criteria in
order to generate a config. If you do not have these in your deployment, you can
manually place the `garbage-collectors` as you would other Manta services. See
`Configuration: Option B` section below for details.

If you've completed this successfully, you can move on to the `Provisioning the
Zones` section below.

## Configuration: Option B

If you do not have enough servers for automatic configuration, or would just
like to manually select servers for `garbage-collector` instances, you should
run:

```
manta-adm show -sj > gc-config.json
```

and then manually add a section like:

```
        "garbage-collector": {
            "acc104a2-db81-4b96-b962-9b51409eadc0": 1
        },
```

to the `gc-config.json` file for each server onto which you'd like to provision
a `garbage-collector` zone. At this point you're ready to move on to the section
below.

## Provisioning the Zones

Once you've created a `gc-config.json` config file either manually or using the
generator, you are ready to start creating the `garbage-collector` zones. To do
so, run:

```
manta-adm update gc-config.json
```

A full run might look like:

```
[root@headnode (nightly-2) ~]# manta-adm update gc-config.json
logs at /var/log/manta-adm.log
service "garbage-collector"
  cn "00000000-0000-0000-0000-002590c0933c":
    provision (image acc104a2-db81-4b96-b962-9b51409eadc0)
  cn "cc9ad6da-e05e-11e2-8e23-002590c3f078":
    provision (image acc104a2-db81-4b96-b962-9b51409eadc0)
Are you sure you want to proceed? (y/N): y

service "garbage-collector": provisioning
    server_uuid: 00000000-0000-0000-0000-002590c0933c
     image_uuid: acc104a2-db81-4b96-b962-9b51409eadc0
service "garbage-collector": provisioning
    server_uuid: cc9ad6da-e05e-11e2-8e23-002590c3f078
     image_uuid: acc104a2-db81-4b96-b962-9b51409eadc0
service "garbage-collector": provisioned aefbf103-b815-4f95-84ea-e446c4e9bb50
    server_uuid: 00000000-0000-0000-0000-002590c0933c
     image_uuid: acc104a2-db81-4b96-b962-9b51409eadc0
service "garbage-collector": provisioned a1f48ab2-6c8a-4538-8567-2ca74a9d5b4d
    server_uuid: cc9ad6da-e05e-11e2-8e23-002590c3f078
     image_uuid: acc104a2-db81-4b96-b962-9b51409eadc0
[root@headnode (nightly-2) ~]#
```

At this point you should have `garbage-collector` instances but they will not
yet be collecting any garbage. To have them start doing that, proceed to the
next step.

## Configuring garbage-collectors Shard Assignments

There are 2 parts to updating the `garbage-collector` shard assignments. First
you want to generate an assignment:

```
manta-adm gc gen-shard-assignment > gc-shard-assignment.json
```

you can take a look at this if you'd like to make sure it looks reasonable. It
should look something like:

```
{
    "aefbf103-b815-4f95-84ea-e446c4e9bb50": {
        "GC_ASSIGNED_BUCKETS_SHARDS": [
            {
                "host": "1.buckets-mdapi.nightly.joyent.us",
                "last": true
            }
        ],
        "GC_ASSIGNED_SHARDS": [
            {
                "host": "1.moray.nightly.joyent.us",
                "last": true
            }
        ]
    },
    "a1f48ab2-6c8a-4538-8567-2ca74a9d5b4d": {
        "GC_ASSIGNED_BUCKETS_SHARDS": [
            {
                "host": "2.buckets-mdapi.nightly.joyent.us",
                "last": true
            }
        ],
        "GC_ASSIGNED_SHARDS": [
            {
                "host": "2.moray.nightly.joyent.us",
                "last": true
            }
        ]
    }
}
```

which will mean that the first `garbage-collector` is responsible for the shards
`1.buckets-mdapi` and `1.moray` and the second `garbage-collector` is
responsible for the shards `2.buckets-mdapi` and `2.moray`. If this looks fine,
you can apply the configuration by running:

```
manta-adm gc update gc-shard-assignment.json
```

which will apply the SAPI metadata changes in order to make the new
configuration active. You should be able to confirm this by running:

```
manta-oneach -s garbage-collector "json dir_shards buckets_shards < /opt/smartdc/manta-garbage-collector/etc/config.json"
```

At this point garbage collection should be working for all deleted objects in
both buckets and directory-style Manta.

## Configuring garbage-collector Tunables

With previous versions of GC, there were a large number of tunables on the SAPI
`garbage-collector` service that are no longer relevant for GCv2 these include:

 * `GC_ASSIGNED_BUCKET`
 * `GC_ASSIGNED_SHARDS`
 * `GC_CACHE_CAPACITY`
 * `GC_CONCURRENCY`
 * `GC_INSTR_UPLOAD_BATCH_SIZE`
 * `GC_INSTR_UPLOAD_FLUSH_DELAY`
 * `GC_INSTR_UPLOAD_PATH_PREFIX`
 * `GC_RECORD_DELETE_BATCH_SIZE`
 * `GC_RECORD_DELETE_DELAY`
 * `GC_RECORD_READ_BATCH_SIZE`
 * `GC_RECORD_READ_SORT_ATTR`
 * `GC_RECORD_READ_SORT_ORDER`
 * `GC_RECORD_READ_WAIT_INTERVAL`

these can be removed once all your instances are updated to GCv2.

The values on the SAPI *instance* for each garbage collector can have the
following:

      `GC_ASSIGNED_BUCKETS_SHARDS`
      `GC_ASSIGNED_SHARDS`

which should *NOT* be removed. These are managed by the `manta-adm gc update`
command.

There are currently the following tunables available for setting in SAPI for the
`garbage-collector` service:

 * `GC_BUCKETS_BATCH_INTERVAL_MS` - The number of milliseconds that
   `garbage-buckets-consumer` will wait between GC batch collections from
   buckets-mdapi. Increasing this value will decrease the frequency of requests.
   In the config file this parameter is `options.buckets_batch_interval_ms`.
   The default value is 60000 (1 minute).

 * `GC_DIR_BATCH_SIZE` - The number of records to read each time a GC batch is
   requested from Moray by the `garbage-dir-consumer`. This value will be used
   as the `limit:` parameter in the findObjects requests. Do not set this to
   more than 1000. In the config file this parameter is
   `options.dir_batch_size`. The default value is 200.

 * `GC_DIR_BATCH_INTERVAL_MS` - The number of milliseconds that
   `garbage-dir-consumer` will wait between GC batch collections from Moray.
   Increasing this will decrease the frequency of requests. In the config file
   this option is `options.dir_batch_interval_ms`. The default value is 60000 (1
   minute).

 * `GC_MPU_BATCH_INTERVAL_MS` - The number of milliseconds that
   `garbage-mpu-cleaner` will wait after each run before we start the next one.
   Longer values will decrease the frequency of queries to Moray and Electric
   Moray at the expense of longer delays between `mpu commit` or `mpu abort` and
   garbage collection. In the config file this option is
   `options.mpu_batch_interval_ms`. The default value is 60000 (1 minute).

 * `GC_MPU_BATCH_SIZE` - The number of MPU finalize records (manta_uploads Moray
   bucket) to read on each "run" of the `garbage-mpu-cleaner`. Increasing this
   will cause the `garbage-mpu-cleaner` to collect more garbage on each run
   which will increase the load on Moray and Electric Moray but will allow more
   garbage to be processed on each run. In the config file this option is
   `options.mpu_batch_size`. The default value is 200.

 * `GC_MPU_CLEANUP_AGE_SECONDS` - The number of seconds old a `manta_upload`
   record needs to be before it is cleaned up by `garbage-mpu-cleaner`.
   These records are required in order for the upload to be visible in for
   example an `mmpu list`. Once the record has been "cleaned", it will no longer
   appear as an "upload". As such, we need to provide a window after `commit` or
   `abort` where the user can still view their upload status before we clean the
   record up. Increasing this value will increase the length of time before
   garbage is cleaned up. In the config file this option is
   `options.mpu_cleanup_age_seconds`. The default value is 300 (5 minutes).


In order to set these values you can use the commands on the headnode:

```
GC_SERVICE_UUID=$(sdc-sapi /services?name=garbage-collector | json -H 0.uuid)
echo "{\"metadata\": {\"GC_DIR_BATCH_SIZE\": 200}}" \
   | sapiadm update ${GC_SERVICE_UUID}
```

Replacing `GC_DIR_BATCH_SIZE` with one of the valid tunables above, and
replacing the number `200` with the intended value of that tunable.

If you set these in SAPI, `config-agent` will update the appropriate config file
paramter in the config file which lives at
`/opt/smartdc/manta-garbage-collector/etc/config.json`.

