# Basic Functionality Demo

XXX 2019-10-28 A lot of this is no longer relevant for Manta v2. Needs to be rewritten.

This demo assumes that a reasonably up-to-date Manta deployment. It will cover
the intended usage of the garbage-collector. This includes:

* Disabling snaplinks
* Deploying changes necessary to create the `manta_fastdelete_queue`
(MANTA-3764)
* Deploying the changes necessary to use the new delete queue in the
`deletestorage` webapi codepath (MANTA-3774).
* Deploying the garbage collection component introduced in MANTA-3776.

We will conclude with a walkthrough of an objects lifecycle in a Manta with the
garbage-collector deployed: from object creation to removal of the on-disk
backing file.

## Disabling Snaplinks

The Manta API supports snaplinks with the `mln` command in the
[node-manta](https://github.com/joyent/node-manta) repo. Snaplinks pose a
problem for garbage-collection because their use implies potentially many
references to any backing file for an object. A more involved garbage-collection
implementation that deals with the presence of snaplinks is described in
[RFD-123](https://github.com/joyent/rfd/blob/master/rfd/0123/README.md). This
implementation relies on a SAPI value introduced in this
[commit](https://github.com/joyent/manta-muskie/commit/760857dbf5d30f5734b4ac21e097628824bb6569)
to disable snaplinks on a per-account basis. 

Any delete records you intend to process with the garbage-collector must have
been created by an account for which snaplinks are disabled in the target Manta
deployment.

To disable snaplinks for an account, first locate the UFDS uuid for the account.
Then, from the headnode of the Manta deployment, run the following:

```
headnode$ MANTA_UUID=$(sdc-sapi /applications?name=manta | json -Hag uuid)
headnode$ sapiadm get $MANTA_UUID > /var/tmp/manta.json
```

Edit `/var/tmp/manta.json`, adding the following field to the `metadata`
sub-object:

```
"ACOUNTS_SNAPLINKS_DISABLED": [
            {
                "uuid": "edfb68bf-c9bd-c024-ed9a-f871d212d314"
                "last": true
            }
]
```

Each object in `ACCOUNTS_SNAPLINKS_DISABLED` array corresponds to an account
that has: 

1. Never used snaplinks
2. Will not use snaplinks for the duration that the garbage-collector is
deployed.

Of note is that (2) is enforced by the change added in MANTA-3768. (1) must be
independently verified. 

You may specify multiple accounts in the `ACCOUNTS_SNAPLINKS_DISABLED` array,
but only the last object may contain the `last` field. This format is necessary
for the config-agent to correctly update the service configurations of Webapi
and Garbage-collector instances in the deployment. 

Once you have completed this step, you will see any attempts to:

1. Create a snaplink to an object created by the disabled account
2. Create a snaplink owned by the disabled account

fail with a `SnaplinksDisabledError`. 

## Creating the `manta_fastdelete_queue`

The garbage-collector requires the `manta_fastdelete_queue` to be present on all
shards containing objects that need to be deleted. Muskie is the component
responsible for creating this table on start-up through
[node-libmanta](https://github.com/joyent/node-libmanta/blob/master/lib/moray.js#L72).

In order to create the table, first import a recent Muskie image and restart at
least one Muskie process in your deployment. While creating the
`manta_fastdelete_queue` requires only a single Muskie restart, rolling out the
snaplink prevention and `deletestorage` route changes require that every single 
Webapi instance pick up the change. It is recommended that these changes
be combined into a single update so that only one restart is necessary. 

## Use the new `manta_fastdelete_queue` in `deletestorage` routes

On deletion, objects are moved from the `manta` table to the `manta_delete_log`
via a Moray 'post' trigger
[installed](https://github.com/joyent/node-libmanta/blob/master/lib/moray.js#L280-L329) 
by Muskie called `recordDeleteLog`. The trigger is created by Muskie on startup as 
part of the `manta` table Moray schema. It is executed in the same transaction that 
deletes the corresponding row of the `manta` table. 

The `recordDeleteLog` trigger was updated in MANTA-3774 to insert a record into
the `manta_fastdelete_queue` instead of the `manta_delete_log` if a header
indicating that the object was created by an account that does not have
snaplinks enabled is present. This header is created by Muskie, which knows how
to set this header because it has access to the `ACCOUNTS_SNAPLINKS_DISABLED`
SAPI value.

Deploying this change requires a Muskie restart. It is recommended that the
restart also apply the other Muskie change described in this document.

## Deploying the garbage-collector

The `garbage-collector`, introduced in MANTA-3776, is a new SAPI service
deployed in a zone much like `pgstatsmon` or `reshard`. `garbage-collector`
zones may be deployed redundantly, much like `moray` or `electric-moray`, with
the `manta-adm` tool. 

The only prerequisite to deploying a garbage-collector zone is that the operator
sets two SAPI variables in the Manta application metadata object describing the
target deployment: `GC_SHARD_NUM_LO`, and `GC_SHARD_NUM_HI`. These variables
describe the range of shards that new `garbage-collector` instances should read
delete records from by default. The operator may override these variables on a
per-instance basis by setting them in the metadata of each garbage-collector
instance metadata. 

To set these variables, run the following:

```
headnode$ MANTA_UUID=$(sdc-sapi /applications?name=manta | json -Hag uuid)
headnode$ sapiadm get $MANTA_UUID > /var/tmp/manta.json
```

Modify `/var/tmp/manta.json` with the additions:
```
"metadata": {
	...
	"GC_SHARD_NUM_LO": 1
	"GC_SHARD_NUM_HI": 2
}
```

Finally, run the `sapiadm update`:

```
headnode$ sapiadm update $MANTA_UUID -f /var/tmp/manta.json
```

Operators do not need to restart any services at this point provided that no
`garbage-collector` zones that need these updates are already running.

To import the latest garbage-collector image:

```
headnode$ IMAGE_UUID=$(updates-imgadm list name=manta-garbage-collector -H -o \
	uuid --latest)
headnode$ sdc-imgadm import $IMAGE_UUID -S https://updates.joyent.com
```

Finally:

```
headnode$ manta-adm show -sj > config.json
```

Add 

```
"garbage-collector": {
	"$IMAGE_UUID": N
}
```

To the configuration, and run the update:

```
manta-adm update config.json -y
```

At this point, the manta deployment will have `N` `garbage-collector` instances,
each of which will start polling shards `GC_SHARD_NUM_LO` through
`GC_SHARD_NUM_HI`, for delete records in the `manta_fastdelete_queue`. You may 
verify that the `garbage-collector` processes are running with the following
`manta-oneach` command:

```
headnode$ manta-oneach -s garbage-collector "curl -X GET localhost:2020/ping"
SERVICE           ZONE      OUTPUT
garbage-collector f812ef45  {"ok":true,"when":"2018-07-30T18:19:11.253Z"}
```

## Demo

This demo assumes that the operator has followed the deployment steps described
in the previous section of this document. The target deployment should meet the
following requirements:

* At least one account, which will be used to create and remove objects in this
demo, should have snaplinks disabled.
* All Muskies should be updated to use the modified `deletestorage` path. 
* The `manta` table `recordDeleteLog` post trigger should be updated with the
most recent version of the trigger. This can be checked by logging into any
Postgres primary and running:
```
psql -d moray -U moray -t -P format=unaligned -c \"select post from buckets_config
where name='manta'\" | grep manta_fastdelete_queue
```
Which should print out the new trigger.

### Pause the GC Workers

While this step is not necessary, it does make it easier to demonstrate the
lifecycle of an object with the new delete mechanism. All GC workers can be
paused with the following:

```
headnode$ manta-oneach -s garbage-collector "curl -X POST
localhost:2020/workers/pause" 
SERVICE           ZONE     OUTPUT                           
garbage-collector f812ef45 {"ok":true,"when":"2018-07-30T18:28:48.864Z"}
```

At this point, no component of the system will be processing the new
`manta_fastdelete_queue`, so we can inspect that objects are transferred
appropriately.

### Create test object

We will start by creating an object with an account that has snaplinks disabled.
It is important that the account have snaplinks disabled, otherwise the delete
records for objects created by the account will be put in the
`manta_delete_log`. The new garbage-collector component can process records in
the `manta_delete_log`, but only if they were created by accounts for which
snaplinks are disabled. For simplicity, and to demonstrate the full codepath, we
will stick with the `manta_fastdelete_queue` in this demo.  

To verify that the account being used has snaplinks disabled:

```
headnode$ ACCOUNT_UUID=$(sdc-mahi /account/$MANTA_USER | json -Hag account.uuid)
headnode$ sdc-sapi /applications?name=manta | json -Hag \
	metadata.ACCOUNTS_SNAPLINKS_DISABLED | json -a uuid | grep $ACCOUNT_UUID
```

The above command  should print out a uuid matching that of the `MANTA_USER` 
environment variable of the session being used to create the object.

Create a random object and upload it to Manta: 
```
devzone$ cat /dev/urandom | head -c 512000 > /tmp/gc-test-object
devzone$ mput -f /tmp/gc-test-object ~~/stor/gc-test-object
```

In another window, we intercept the Muskie log entry for the `putobject` route,
which indicates where the metadata for the new object is stored:
```
...
entryShard: "tcp://1.moray.orbit.example.com:2020"
parentShard: "tcp://1.moray.orbit.example.com:2020"
```

Logging into shard one, we can take a look at the row of representing the object
metadata: 
```
headnode$ manta-login postgres 0
postgres$ psql -d moray -U postgres -t -P format=unaligned -c \
	"select _value from manta where name='gc-test-object'" | json
{                            
  "dirname": "/edfb68bf-c9bd-c024-ed9a-f871d212d314/stor", 
  "key": "/edfb68bf-c9bd-c024-ed9a-f871d212d314/stor/gc-test-object",                                                 
  "headers": {},             
  "mtime": 1532984371479,    
  "name": "gc-test-object",  
  "creator": "edfb68bf-c9bd-c024-ed9a-f871d212d314",       
  "owner": "edfb68bf-c9bd-c024-ed9a-f871d212d314",         
  "roles": [],               
  "type": "object",          
  "contentLength": 512000,   
  "contentMD5": "KTl8ckUWbxxkKkEI2bIiHw==",                
  "contentType": "application/octet-stream",               
  "etag": "d0fa5fd0-4586-c205-ef53-96558fc4f30e",          
  "objectId": "d0fa5fd0-4586-c205-ef53-96558fc4f30e",      
  "sharks": [                
    {                        
      "datacenter": "coal-1",                              
      "manta_storage_id": "3.stor.orbit.example.com"       
    },                       
    {                        
      "datacenter": "coal-1",                              
      "manta_storage_id": "4.stor.orbit.example.com"       
    }                        
  ],                         
  "vnode": 41314             
}
```

Having saved all the relevant information, we can delete the object: 
```
headnode$ mrm ~~/stor/gc-test-object
```

And verify that a corresponding record has been created in the
`manta_fastdelete_queue` on the shard 1 primary:

```
psql -d moray -U moray -P format=unaligned -t -c \
	"select _value from manta_fastdelete_queue where _key='c419d24c-cbf5-6097-b628-ea0de57dd61d'"
{
  "dirname": "/edfb68bf-c9bd-c024-ed9a-f871d212d314/stor",
  "key": "/edfb68bf-c9bd-c024-ed9a-f871d212d314/stor/gc-test-object",
  "headers": {},
  "mtime": 1532984371479,
  "name": "gc-test-object",
  "creator": "edfb68bf-c9bd-c024-ed9a-f871d212d314",
  "owner": "edfb68bf-c9bd-c024-ed9a-f871d212d314",
  "roles": [],
  "type": "object",
  "contentLength": 512000,
  "contentMD5": "KTl8ckUWbxxkKkEI2bIiHw==",
  "contentType": "application/octet-stream",
  "etag": "d0fa5fd0-4586-c205-ef53-96558fc4f30e",
  "objectId": "d0fa5fd0-4586-c205-ef53-96558fc4f30e",
  "sharks": [
    {
      "datacenter": "coal-1",
      "manta_storage_id": "3.stor.orbit.example.com"
    },
    {
      "datacenter": "coal-1",
      "manta_storage_id": "4.stor.orbit.example.com"
    }
  ],
  "_etag": "366DFAB2"
}
```
One thing to notice about the above command is that we used the objectId from
the metadata entry to select the delete record. This bit garuantees that two
objects uploaded to the same Manta path will not overlap in the
`manta_fastdelete_queue`.

Note that, at this point, the record still exists because the GC workers are
paused. When we resume the workers the delete record will disappear.

### Tunables

Before we resume the GC workers and demonstrate that the procedure works, we
will set some worker tunables that will make it easier to demonstrate the
particular case of one object being garbage collected. 

This step would normally not be required in a real deployment with continuously
growing `manta_fastdelete_queue`s, and even in this context, it is just a
convenience measure. 

Since we only anticipate deleting a single object, we will set the batch sizes
of the garbage collector instances to 1:

```
manta-oneach -s garbage-collector "curl -X POST localhost:2020/shards -H 'content-type: application/json' 
	-d '{
		\"record_read_batch_size\": 1,
		\"record_delete_batch_size\": 1, 
		\"record_read_wait_interval\": 1200000
	}'"
SERVICE           ZONE     OUTPUT
garbage-collector f812ef45 {"ok":true,"when":"2018-07-30T19:37:50.554Z"}
```

The reason we modify the wait interval is to give ourselves time to demonstrate
the action of a garbage-collector before it polls the `manta_fastdelete_queue`
again.

We will also want to set the instruction upload batch size to 1, so that we can
easily isolate the instruction that we are looking for: 

```
manta-oneach -s garbage-collector "curl -X POST localhost:2020/mako -H 'content-type: application/json' 
	-d '{
		\"instr_write_batch_size\": 1
	}'"
SERVICE           ZONE     OUTPUT
garbage-collector f812ef45 {"ok":true,"when":"2018-07-30T19:41:55.704Z"}
```

Note: these tunables are not recommended for a real deployment, which will
insert many records into the `manta_fastdelete_queue` per second. 

### Resuming the Workers

Now that everything is set up, we will resume the workers and observe two
results:

1. The delete record for the object with object id
`d0fa5fd0-4586-c205-ef53-96558fc4f30e` was removed from the
`manta_fastdelete_queue`
2. The appropriate delete instructions were uploaded to Manta.

First, we resume the workers:

```
headnode$ manta-oneach -s garbage-collector "curl -X POST localhost:2020/workers/resume"
SERVICE          ZONE     OUTPUT
garbage-collector f812ef45 {"ok":true,"when":"2018-07-30T21:14:19.198Z"}
```

### Observing the Instructions

The garbage-collector communicates objects to delete to storage nodes via
instructions that are uploaded to Manta and read by the `mako_gc.sh` script in
the
[manta-mako](https://github.com/joyent/manta-mako/blob/master/bin/mako_gc.sh)
repository. By default, a cron job running the script on a storage node with 
id `$STORAGE_ID` looks for instructions uploaded to the Manta path:
```
/poseidon/stor/manta_gc/mako/$STORAGE_ID
```

As part of this demo, we will want to see that the appropriate instructions are
uploaded to these locations. We can use the `-t` flag to mls to give us the
basename of the most recently uploaded instruction in both locations and the
`mget` that object, comparing the objectId of the instruction specified to see
that it matches the delete record added in the `manta_fastdelete_queue`:

```
ops$ mget $(mls -t /poseidon/stor/manta_gc/mako/3.stor.orbit.example.com | \
	head -1)
mako    3.stor.orbit.example.com        edfb68bf-c9bd-c024-ed9a-f871d212d314	d0fa5fd0-4586-c205-ef53-96558fc4f30e
```

We can do the same for the `4.stor.orbit.example.com` storage node:

```
ops$ mget $(mls -t /poseidon/stor/manta_gc/mako/4444.stor.orbit.example.com | \
	head -1)
mako    4.stor.orbit.example.com        edfb68bf-c9bd-c024-ed9a-f871d212d314	d0fa5fd0-4586-c205-ef53-96558fc4f30e
```

As is evident from both cases above, we have an instruction to remove the
appropriate object, which is owned by the appropriate account.


### Observing the `manta_fastdelete_queue` 

Since the instruction to remove the row has been uploaded to Manta, the garbage
collector should have safely removed the delete record from the
`manta_fastdelete_queue`. We can verify this by going back to shard 1 and
running:

```
postgres$ psql -d moray -U moray -P format=unaligned -t -c 
	"select _value from manta_fastdelete_queue where _key='d0fa5fd0-4586-c205-ef53-96558fc4f30e'" | json
postgres$
```

This command returns empty because there is indeed no longer a record for the
object with the `objectid` we are looking for.

### Observing `mako_gc.sh`

Here is the current situation:

* We created an object with objectid `d0fa5fd0-4586-c205-ef53-96558fc4f30e` that
is owned by the account `edfb68bf-c9bd-c024-ed9a-f871d212d314`, and then removed
it.
* We observed that the removal created an entry in the `manta_fastdelete_queue`.
* We started the GC workers and observed that the appopriate delete instructions
were created and the un-needed delete record was removed from the
`manta_fastdelete_queue`. 

What remains is to run the `mako_gc.sh` script. This is the component
responsible for reading the instructions the garbage-collector uploaded and
remove the corresponding backing file from the filesystem. Really, this happens
in two steps:

1. On the first run of `mako_gc.sh`, any objects marked for deletion are moved
from their 'live' location in
`/manta/edfb68bf-c9bd-c024-ed9a-f871d212d314` (for objects owned by this
account), to a 'tombstone' directory named for the day they were processed by
the `mako_gc.sh` script.
2. On a subsequent run of the `mako_gc.sh` script, object files living in
tombstone directories that are at least 21 days old get permanently removed from
the filesystem. 

First, we will verify that the backing files for the object we have uploaded
exist on both storage nodes referred to by its metadata record:
```
3.stor.orbit.example.com$ stat /manta/edfb68bf-c9bd-c024-ed9a-f871d212d314/d0fa5fd0-4586-c205-ef53-96558fc4f30e
  File:
'/manta/edfb68bf-c9bd-c024-ed9a-f871d212d314/d0fa5fd0-4586-c205-ef53-96558fc4f30e'                            
  Size: 512000          Blocks: 1029       IO Block: 131072 regular file
...
4.stor.orbit.example.com$ stat /manta/edfb68bf-c9bd-c024-ed9a-f871d212d314/d0fa5fd0-4586-c205-ef53-96558fc4f30e
  File:
'/manta/edfb68bf-c9bd-c024-ed9a-f871d212d314/d0fa5fd0-4586-c205-ef53-96558fc4f30e'
  Size: 512000          Blocks: 1029       IO Block: 131072 regular file
...
```

The grace period of 21 days is enforced by a count of the number of directories
in `/manta/tombstone` we should keep. By default, `mako_gc.sh` runs once a day
and creates such a directory on each run. Hence, by limiting the number of
tombstone directories to keep to 0, (see usage of this
[variable](https://github.com/joyent/manta-mako/blob/master/bin/mako_gc.sh#L49)),
we can arrange for object to be removed as soon as they are deleted.

After that modification, we can initiate the gc script on both nodes as follows:
```
3.stor.orbit.example.com$ /opt/smartdc/mako/bin/mako_gc.sh &> /var/tmp/mako-gc.log &
4.stor.orbit.example.com$ /opt/smartdc/mako/bin/mako_gc.sh &> /var/tmp/mako-gc.log & 
```

On the deployment that this demo is running on, these storage nodes have not yet
been used for anything, so it is not too hard to see what is going on by looking
at an entire log:
```
2018-07-31T00:11:53.000Z: mako_gc.sh (85891): info: starting gc
2018-07-31T00:11:54.000Z: mako_gc.sh (85891): info: Processing manta object
/manta_gc/mako/3.stor.orbit.example.com/2018-07-30-21:14:19-f812ef45-3877-4b4f-8465-823901b277ea-X-cb7facdd-860c-4a53-9605-8a5fe2ff75e7-mako-3.stor.orbit.example.com
2018-07-31T00:11:54.000Z: mako_gc.sh (85891): info: Processing mako
3.stor.orbit.example.com        edfb68bf-c9bd-c024-ed9a-f871d212d314	d0fa5fd0-4586-c205-ef53-96558fc4f30e
{"audit":true,"name":"mako_gc","level":30,"msg":"audit","v":0,"time":"2018-07-31T00:11:54.000Z","pid":85891,"hostname":"4f6b5fbf-69fe-4e19-9c36-c06b8bf2ef57","alreadyDeleted":"false","objectId":"/manta/edfb68bf-c9bd-c024-ed9a-f871d212d314/d0fa5fd0-4586-c205-ef53-96558fc4f30e","tomb":"/manta/tombstone/2018-07-31","processed":1}
2018-07-31T00:11:55.000Z: mako_gc.sh (85891): info: success processing
/manta_gc/mako/3.stor.orbit.example.com/2018-07-30-21:14:19-f812ef45-3877-4b4f-8465-823901b277ea-X-cb7facdd-860c-4a53-9605-8a5fe2ff75e7-mako-3.stor.orbit.example.com.
2018-07-31T00:11:55.000Z: mako_gc.sh (85891): info: starting tombstone directory
cleanup                              
2018-07-31T00:11:55.000Z: mako_gc.sh (85891): info: cleaning up
/manta/tombstone/2018-07-30                           
2018-07-31T00:11:55.000Z: mako_gc.sh (85891): info: cleaning up
/manta/tombstone/2018-07-31                           
{"audit":true,"name":"mako_gc","level":30,"error":false,"msg":"audit","v":0,"time":"2018-07-31T00:11:55.000Z","pid":85891,"cronExec":1,"hostname":"4f6b5fbf-69fe-4e19-9c36-c06b8bf2ef57","fileCount":"1","objectCount":"0","tombDirCleanupCount":"2"}
```

We can see that the `mako_gc.sh` script processed the instruction object the
garbage-collector created. Finally we verify that the backing file has been
removed:
```
3.stor.orbit.example.com$ stat /manta/edfb68bf-c9bd-c024-ed9a-f871d212d314/d0fa5fd0-4586-c205-ef53-96558fc4f30e
stat: cannot stat
'/manta/edfb68bf-c9bd-c024-ed9a-f871d212d314/d0fa5fd0-4586-c205-ef53-96558fc4f30e':
No such file or directory
3.stor.orbit.example.com$ ls /manta/tombstone
3.stor.orbit.example.com$

4.stor.orbit.example.com$ stat /manta/edfb68bf-c9bd-c024-ed9a-f871d212d314/d0fa5fd0-4586-c205-ef53-96558fc4f30e
stat: cannot stat
'/manta/edfb68bf-c9bd-c024-ed9a-f871d212d314/d0fa5fd0-4586-c205-ef53-96558fc4f30e':
No such file or directory
4.stor.orbit.example.com$ ls /manta/tombstone
4.stor.orbit.example.com$
```

We can also see that the instruction object that the garbage-collector created
has been removed:
```
ops$ mls /poseidon/stor/manta_gc/mako/3.stor.orbit.example.com
ops$ mls /poseidon/stor/manta_gc/mako/4.stor.orbit.example.com
ops$
```
