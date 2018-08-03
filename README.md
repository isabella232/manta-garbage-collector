<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright (c) 2018, Joyent, Inc.
-->

# Manta Garbage Collector

This repository is part of the Joyent Manta project.  For contribution
guidelines, issues, and general documentation, visit the main
[Manta](http://github.com/joyent/manta) project page.

This is the garbage collection system for Manta. It comprises of a
service for processing metadata records pointing to deleted objects
and then ensuring the corresponding backing files are removed from the
appropriate Mako zones.

# Metrics

The garbage collector exposes a number of application-level metrics, in addition
to node-fast metrics for the two RPCs it uses: `findObjects`, and `deleteMany`.

| name                      | type      | help                                |
|:-------------------------:|:---------:|:-----------------------------------:|
| gc_cache_entries          | gauge     | total number of cache entries       |
| gc_delete_records_read    | histogram | records read per `findObjects`      |
| gc_delete_records_cleaned | histogram | records cleaned per `deleteMany`    |
| gc_mako_instrs_uploaded   | histogram | instructions uploaded per Manta PUT |


# HTTP Management Interface

Each garbage collector process starts a restify http server listening on port
2020 by default (this is configurable). The server supports the following
endpoints:

```
Request:

GET /ping

Response:

{
	ok: boolean
	when: ISO timestamp
}
```

Endpoints listed under `/workers` implement worker control. Supported
actions include **listing**, **pausing**, and **resuming** the
workers. Newly created workers start in the running phase, even if
other workers are paused.

```
Request:

GET /workers/get

Response:

{
	"shard-url": {
		"bucket": [
		    {
			"component": "worker",
			"state": "running|paused|error",
			"actors": [
				{
					"component": "reader",
					"state": "running|waiting|error"
				},
				{
					"component": "transformer",
					"state": "running|waiting|error",
					"cached": integer
				},
				{
					"component": "instruction uploader",
					"state": "running|waiting|error"
				},
				{
					"component": "cleaner",
					"state": "running",
					"cached": integer
				}
			]
		    }
		...
		]
	}
	...
}
```
The `shard-url` variable will be a full domain like `2.moray.orbit.example.com`.
`bucket` will generally be either `manta_fastdelete_queue` or
`manta_delete_log`.

Pause all workers:
```
POST /workers/pause

Response:

{
	ok: boolean,
	when: ISO timestamp
}
```

Resume all workers:
```
POST /workers/resume

Response:

{
	ok: boolean,
	when: ISO timestamp
}
```

Retrieve tunables relevant to the upload of Mako GC instructions:
```
GET /mako

Response:

{
	"instr_upload_batch_size": integer,
	"instr_upload_flush_delay": integer,
	"instr_upload_path_prefix": string
}
```

* `instr_upload_batch_size`:  the target number of lines to include in each
instruction object.
* `instr_upload_flush_delay`: number of milliseconds between periodic cached
  instruction uploads. Used to prevent cache entries from waiting around too
long if there aren't sufficient enough delete records in the bucket we're
reading from to reach the batch size.
* `instr_upload_path_prefix`: The Manta path to which the garbage collector will
upload Mako GC instruction objects. If the value of this tunable is `$PATH`,
then the garbage collector will upload objects for shark `$SHARK` in
`$PATH/$SHARK`, creating the `$SHARK` directory if necessary. In practice, we'll
set this to `/poseidon/stor/manta_gc/mako`, which is where instruction objects
are uploaded by the offline GC process.

The following request allows an operator to modify any of the tunables returned
by the previous endpoint.
```
POST /mako
Content-Type: application/json

{
	"instr_upload_batch_size": integer,
	"instr_upload_flush_delay": integer,
	"instr_upload_path_prefix": string
}

Response:

{
	ok: boolean,
	when: ISO timestamp
}
```

Retrieve shard-specific GC configuration. This configuration is used by all GC
workers that are retrieving records from the shard identified in the url.
Broadly, the configuration describes what buckets on the shard to read, and how
to read them. The `:shard` url parameter is a number describing which shards
configuration to read.
```
GET /shards/:shard

Response:

{
	"buckets": [
		{
			"bucket": "manta_fastdelete_queue|manta_delete_log",
			"concurrency": integer
		}
		...
	],
	"record_read_batch_size": integer,
	"record_read_wait_interval": integer (ms),
	"record_read_sort_attr": "_mtime",
	"record_read_sort_order": "asc|ASC|desc|DESC",
	"record_delete_batch_size": integer,
	"record_delete_delay": integer (ms)
}

Failure:

{
	ok: false,
	err: 'No GC worker for shard ":shard"'
}
```
* `buckets`: An array of objects, each of which describes how many GC workers
should be allocated to processing records in that bucket. Today, we only support
reading records from `manta_fastdelete_queue` and `manta_delete_log`.
* `record_read_batch_size`: The number of records to read with each Moray
`findObjects` RPC. This is fed into the `limit` option passed to that RPC.
* `record_read_sort_attr`: Which attribute to sort by when processing records.
* `record_read_sort_ord`: What order to use when sorting records.
* `record_delete_batch_size`: How many records to delete with a single Moray
`deleteMany` RPC.
* `record_delete_delay`: The threshold amount of time a worker will wait since
the last successful `deleteMany` RPC before issuing a 'periodic' `deleteMany`.

Modify any of the tunables returned by the previous endpoint by passing a JSON
object describing the new desired values. This will affect the behavior of all
workers reading records from shard `:shard`, creating or destroying workers if
an entry is added to, or removed from, the `buckets` array.
```
POST /shards/:shard
Content-Type: application/json

{
	"buckets": [
		{
			"bucket": "manta_fastdelete_queue|manta_delete_log",
			"concurrency": integer
		}
		...
	],
	"record_read_batch_size": integer,
	"record_read_wait_interval": integer (ms),
	"record_read_sort_attr": "_mtime",
	"record_read_sort_order": "asc|ASC|desc|DESC",
	"record_delete_batch_size": integer,
	"record_delete_delay": integer (ms)

}

Response:

{
	ok: boolean,
	when: ISO timestamp
}

Failure:

{
	ok: false,
	err: 'No GC worker for shard ":shard"'
}
```

The `/shards` endpoint, described below should be used for two purposes:
1. To modify the configuration of all shards. This is equivalent to applying the
changes included in the JSON body to all shards the garbage collector is
processing via the `/shards/:shard` endpoint. Changes applied in this manner
affect the 'default' shard configuration which is used by any newly created
workers.
2. To modify the range of shards that the garbage collector is responsible for
processing.

```
POST /shards
Content-Type: application/json

{
	"interval": [lo, hi],
	"buckets": [
		{
			"bucket": "manta_fastdelete_queue|manta_delete_log",
			"concurrency": integer
		}
		...
	],
	"record_read_batch_size": integer,
	"record_read_wait_interval": integer (ms),
	"record_read_sort_attr": "_mtime",
	"record_read_sort_order": "asc|ASC|desc|DESC",
	"record_delete_batch_size": integer,
	"record_delete_delay": integer (ms)

}

Response:

{
	ok: boolean
	when: ISO timestamp
}
```
As described above, changes to configuration applied via this endpoint are
applied to all existing and future workers.

* `interval`: specifies a range of shard numbers the garbage collector should
process delete records from. This endpoint enforces that `lo` is less than or
equal to `hi`.
	* If `lo == hi`, then only the one shard identified by `lo` will be
	  GC'd.
	* If `lo < hi`, all shards in the range `lo..hi` will be GC'd from.

If the range includes a shard number that does not exist in the Manta
deployment, or the request triggers the creation of some number of workers for
shards that are unresponsive, no change to the range of shards processed be the
GC process will occur.

```
GET /shards

Response:
{
	"interval": [lo, hi],
	"buckets": [...]
}
```
Note: the configuration returned by this endpoint may differ from the
configuration returned by `GET /shards/:shard` for a specific `:shard` if the
operator has overridden configuration for that shard specifically.

The garbage collector supports a global limit on the total number of cache
entries that garbage collector can hold. The admin interface exposes two
endpoints for reading and modifying this information.

### Some Examples

For example, if the garbage collector were running on the local host and the
operator wanted to change the read batch size for all shards the process is
reading records from, that would look like this:
```
$ curl -X POST -H 'content-type: application/json' localhost:2020/shards \
	-d '{"record_read_batch_size": 10}'
```
If the operator wanted the garbage collector to only process records from the
`manta_fastdelete_queue` on all shards, that would look like this:
```
$ curl -X POST -H 'content-type: application/json' localhost:2020/shards \
	-d '{
	"buckets":[
		{"bucket": "manta_fastdelete_queue", "concurrency": 1},
		{"bucket": "manta_delete_log", "concurrency": 0}
	]}'
```
If the operator wanted to also process `manta_delete_log` records on shard 47,
that would look like this:
```
$ curl -X POST -H 'content-type: application/json' localhost:2020/shards/47 \
	-d '{
		"buckets":[
			{"bucket": "manta_fastdelete_queue", "concurrency": 1},
			{"bucket": "manta_delete_log", "concurrency": 1}
		]
	}'
```

If the operator wanted to GC objects in the shard range [5-10], that command
would look like this:
```
$ curl -X POST -H 'content-type: application/json' localhost:2020/shards \
	-d '{"interval": [5, 10]}'
```

```
GET /cache

Response:

{
	"cache": {
		"capacity": integer,
		"used": integer
	}
}
```
* `cache`: number of allowed cache entries
* `used`: number of cache entries currently used by the collector

```
POST /cache
Content-Type: application/json

{
	"capacity": integer
}

Response:

{
	ok: boolean,
	when: ISO timestamp
}
```

# Configuration

The configuration in `etc/config.json` is an example of how the garbage
collector is configured. Some chunks of this configuration are described below:
* `manta`: A JSON object passed to node-manta's `createClient`.
* `moray`: A JSON object with options passed to node-moray's `createClient`. At
minimum, this must include a resolver.
* `shards`: A JSON object describing which shards, and which buckets on each of
those shards, to process delete records from.
* `creators`: An array of objects with a single field, `uuid`. Any delete
records whose `value.creator` field doesn't match the `uuid` field of some
object in this array will be ignored. This field will be initialized with the
SAPI value `ACCOUNTS_SNAPLINKS_DISABLED`, introduced
[here](https://github.com/joyent/manta-muskie/commit/760857dbf5d30f5734b4ac21e097628824bb6569).
* `params`: A JSON object with two principal sub-objects describing
how records should be read from Moray shards, and how Mako instruction
objects should be uploaded to Manta.
* `address`/`port`: The address and port on which to start the HTTP management
interface described above.
* `capacity`: The total cache capacity (readable and configurable via the
`/cache` endpoint described above).
