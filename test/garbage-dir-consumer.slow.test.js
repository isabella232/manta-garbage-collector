/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

var child_process = require('child_process');
var crypto = require('crypto');
var fs = require('fs');
var path = require('path');

var assert = require('assert-plus');
var EventEmitter = require('events');
var test = require('@smaller/tap').test;
var uuid = require('uuid/v4');
var vasync = require('vasync');
var VError = require('verror');

var GarbageDirConsumer = require('../lib/garbage-dir-consumer.js');

var DATACENTER = 'trashland';
var MORAY_BUCKET = 'manta_fastdelete_queue';
var MORAY_SHARD = '1.moray.test.joyent.us';
var STOR_SUFFIX = '.stor.test.joyent.us';
var TEST_DIR = path.join('/tmp', _randomString() + '.garbage-uploader-test');
var TEST_DIR_INSTRUCTIONS = path.join(TEST_DIR, 'instructions');
var TEST_READ_BATCH_SIZE = 42;

var consumer;
var logs = {
    debug: [],
    error: [],
    info: [],
    trace: [],
    warn: []
};
var logger = {
    child: function _child() {
        return logger;
    },
    debug: function _debug() {
        logs.debug.push(Array.from(arguments));
    },
    error: function _error() {
        logs.error.push(Array.from(arguments));
    },
    info: function _info() {
        logs.info.push(Array.from(arguments));
    },
    trace: function _trace() {
        logs.trace.push(Array.from(arguments));
    },
    warn: function _warn() {
        logs.warn.push(Array.from(arguments));
    }
};
var morayClient;
var runEmitter = new EventEmitter();
var test_dirs_created = false;

function _runHook(obj) {
    runEmitter.emit('run', obj);
}

//
// For obj:
//
//  - .id (optional) should be a number
//  - .key should be e.g. /2873b2b0-7f90-477a-b1ae-4f8d4841dd53/stor/hello.txt
//  - .sharks should be array of numbers, e.g.: [1, 2]
//
function _createRecord(obj) {
    assert.object(obj, 'obj');
    assert.optionalNumber(obj.count, 'obj.count');
    assert.number(obj.id, 'obj.id');
    assert.string(obj.key, 'obj.key');
    assert.array(obj.sharks, 'obj.sharks');

    var count = obj.count || 1;
    var dir;
    var record = {
        bucket: MORAY_BUCKET,
        _id: obj.id,
        _etag: Math.random()
            .toString(16)
            .slice(2)
            .toUpperCase()
            .substr(0, 8),
        _mtime: Date.now(),
        _txn_snap: null,
        _count: count
    };
    var objectId = uuid();
    var ownerId;

    dir = path.dirname(obj.key);
    ownerId = obj.key.split('/')[1];

    record.value = {
        dirname: dir,
        key: obj.key,
        headers: {},
        mtime: record._mtime - Math.floor(Math.random() * 100000),
        name: path.basename(obj.key),
        creator: ownerId,
        owner: ownerId,
        roles: [],
        type: 'object',
        contentLength: Math.floor(Math.random() * 1000000),
        contentMD5: crypto
            .createHash('md5')
            .update(objectId)
            .digest('base64'),
        contentType: 'text/plain',
        etag: objectId,
        objectId: objectId
    };

    record.value.sharks = obj.sharks.map(function _sharkJumper(sId) {
        return {datacenter: DATACENTER, manta_storage_id: sId + STOR_SUFFIX};
    });

    return record;
}

function _createTestDirs(t) {
    var dir;
    var dirs = [TEST_DIR, TEST_DIR_INSTRUCTIONS];
    var i;

    for (i = 0; i < dirs.length; i++) {
        dir = dirs[i];
        fs.mkdirSync(dir);
        // eslint-disable-next-line no-octal
        fs.chmodSync(dir, 0777);
        t.ok(true, 'create ' + dir);
    }

    test_dirs_created = true;
}

function _deleteTestDirs(t) {
    if (test_dirs_created) {
        child_process.execFileSync('/usr/bin/rm', ['-fr', TEST_DIR], {});
        t.ok(true, 'delete ' + TEST_DIR);
    } else {
        t.ok(true, 'did not create test dirs that need deletion');
    }
}

function _randomString() {
    return Math.random()
        .toString(36)
        .slice(2);
}

function _checkValidInstrFile(t, file, cb) {
    fs.readFile(file, {encoding: 'utf8'}, function _onRead(err, data) {
        var base = path.basename(file);
        var expectedStorId;
        var fields;
        var idx;
        var lines = [];

        t.ifError(err, 'should be able to read file ' + base);

        if (!err) {
            lines = data.trim().split('\n');

            t.ok(
                lines.length > 0,
                'should have > 0 lines, saw: ' + lines.length
            );

            for (idx = 0; idx < lines.length; idx++) {
                fields = lines[idx].split('\t');

                t.equal(fields.length, 5, 'should have 5 fields');
                expectedStorId = base.match(
                    /^.*-mako-([0-9]+.*).instruction$/
                )[1];
                t.equal(
                    expectedStorId,
                    fields[0],
                    'should have storId as fields[0]'
                );
                // eslint-disable-next-line no-loop-func
                t.doesNotThrow(function _checkOwner() {
                    assert.uuid(fields[1], 'fields[1]');
                    assert.uuid(fields[2], 'fields[2]');
                }, 'owner and object should be UUIDs');
                t.equal(
                    MORAY_SHARD,
                    fields[3],
                    'fields[3] should be our moray shard'
                );
                t.ok(
                    fields[4].match(/^[0-9]+$/),
                    'fields[4] should be a number (found ' + fields[4] + ')'
                );
            }
        }

        cb();
    });
}

//
// A slightly dumber moray client. This one we can make return what we want.
//
// eslint-disable-next-line no-unused-vars
function DummyMorayClient(opts) {
    var self = this;

    self.emitter = new EventEmitter();
}

DummyMorayClient.prototype.once = function clientOnce(evt, callback) {
    if (evt === 'connect') {
        // Just wait 10ms and pretend we connected
        setTimeout(callback, 10);
        return;
    }

    throw new Error('BOOM');
};

DummyMorayClient.prototype.findObjects = function findObjects(
    bucket,
    filter,
    opts
) {
    var self = this;

    var req = new DummyMorayClientReq();

    self.emitter.emit('findObjects', {
        bucket: bucket,
        filter: filter,
        opts: opts,
        req: req
    });

    return req;
};

DummyMorayClient.prototype.deleteMany = function deleteMany(
    bucket,
    filter,
    callback
) {
    var self = this;

    if (self.emitter.listeners('deleteMany').length > 0) {
        // If there's a listener, they need to call the callback
        self.emitter.emit('deleteMany', bucket, filter, callback);
    } else {
        callback();
    }
};

//
// DumbMorayClient needs to be able to return Dumb Request objects.
//
function DummyMorayClientReq() {
    var self = this;

    self.emitter = new EventEmitter();
}

DummyMorayClientReq.prototype.once = function reqOnce(evt, callback) {
    var self = this;

    self.emitter.once(evt, callback);
};

DummyMorayClientReq.prototype.on = function reqOn(evt, callback) {
    var self = this;

    self.emitter.on(evt, callback);

    if (evt === 'record') {
        // now that we've setup the on('record', ) watcher we want to actually
        // generate some records.
    }
};

DummyMorayClientReq.prototype.emit = function reqEmit(evt, obj) {
    var self = this;

    self.emit(evt, obj);
};

test('create testdirs', function _testCreateTestdirs(t) {
    t.doesNotThrow(function _callCreator() {
        _createTestDirs(t);
    }, 'create test directories');
    t.end();
});

test('create GarbageDirConsumer', function _testCreateDirConsumer(t) {
    morayClient = new DummyMorayClient({shard: MORAY_SHARD});

    consumer = new GarbageDirConsumer({
        config: {
            instance: uuid(),
            options: {
                dir_batch_size: TEST_READ_BATCH_SIZE,
                dir_batch_interval_ms: 1
            }
        },
        instructionRoot: TEST_DIR_INSTRUCTIONS,
        log: logger,
        _morayClient: morayClient,
        morayConfig: {},
        _runHook: _runHook,
        shard: MORAY_SHARD
    });

    t.ok(consumer, 'create GarbageDirConsumer');

    // consumer.start() doesn't call the callback until the first .run()
    // completes, so we need to make sure here that we handle the findObjects
    // and return 'end' so that it looks like it just found nothing and the run
    // completes.
    morayClient.emitter.once('findObjects', function _findObjects(obj) {
        t.ok(true, 'saw findObjects');

        // 10ms is way faster than a real moray
        setTimeout(function _endReq() {
            obj.req.emitter.emit('end');
        }, 10);
    });

    // finish the test once the first run is complete.
    runEmitter.once('run', function _onRun(_obj) {
        t.end();
    });

    consumer.start(function _started(err) {
        t.ifError(err, 'start GarbageDirConsumer');
    });
});

//
// findObjects requests should be happening on each run and should be emitted on
// the consumer.emitter's 'findObjects' channel. So here we just grab the next
// one of these events and make sure the request looks like we expect when we
// don't send any results.
//
test('check findObjects requests', function _testFindObjectsRequests(t) {
    morayClient.emitter.once('findObjects', function _findObjects(obj) {
        t.equal(
            obj.bucket,
            MORAY_BUCKET,
            'Bucket should be manta_fastdelete_queue'
        );
        t.equal(obj.filter, '(_id>=0)', 'Filter should be (_id>=0)');

        // obj.opts looks like: {"limit":42,"sort":{"attribute":"_id","order":"ASC"}}
        t.equal(obj.opts.sort.attribute, '_id', 'sort attribute should be _id');
        t.equal(obj.opts.sort.order, 'ASC', 'sort order should be ASC');
        t.equal(
            obj.opts.limit,
            TEST_READ_BATCH_SIZE,
            'limit should be set to parameter value'
        );

        runEmitter.once('run', function _onRun(_obj) {
            t.ifError(_obj.err, 'run: should be no error');
            t.equal(_obj.deletes, 0, 'run: should be no deletes');
            t.equal(_obj.reads, 0, 'run: should be no reads');
            t.equal(_obj.writes, 0, 'run: should be no reads');

            t.end();
        });

        // 10ms is way faster than a real moray, but we need some delay for the
        // handlers to be setup.
        setTimeout(function _endReq() {
            obj.req.emitter.emit('end');
        }, 10);
    });
});

test('check findObjects requests with results', function _testFindObjectsRequestsResults(t) {
    var expectedFiles = {};
    var ids = [];
    var numObjects = 10;
    var numObjectsReturned = 0;
    var ownerId = uuid();

    // The run will complete after we send 'end'
    runEmitter.once('run', function _onRun(obj) {
        var files = [];

        t.ifError(obj.err, 'run: should be no error');
        t.equal(
            obj.deletes,
            numObjects,
            'run: should be correct number of deletes'
        );
        t.equal(
            obj.reads,
            numObjects,
            'run: should be correct number of reads'
        );
        t.equal(
            obj.writes,
            numObjects * 2,
            'run: should be correct number of writes (copies = 2)'
        );

        // Look at expected and confirm files look like what we expect.
        vasync.forEachPipeline(
            {
                func: function _checkStor(storId, cb) {
                    var dir;

                    dir = path.join(
                        TEST_DIR_INSTRUCTIONS,
                        storId + STOR_SUFFIX
                    );

                    fs.readdir(dir, function _onReaddir(readdirErr, dirFiles) {
                        if (readdirErr) {
                            cb(
                                new VError(
                                    {
                                        cause: readdirErr,
                                        info: {dir: dir, storId: storId},
                                        name: 'FailedInstrReaddir'
                                    },
                                    'Failed to readdir ' + dir
                                )
                            );
                            return;
                        }

                        files = files.concat(
                            dirFiles.map(function _makeAbsolute(p) {
                                return path.join(dir, p);
                            })
                        );

                        cb();
                    });
                },
                inputs: Object.keys(expectedFiles)
            },
            function _foundAllFiles(findErr) {
                t.ifError(findErr, 'found all expected instr files');
                if (findErr) {
                    t.end();
                    return;
                }

                vasync.forEachPipeline(
                    {
                        func: function _checkInstrFile(file, cb) {
                            _checkValidInstrFile(t, file, cb);
                        },
                        inputs: files
                    },
                    function _checkedAllFiles(checkErr) {
                        t.ifError(checkErr, 'checked all instr files');
                        t.end();
                    }
                );
            }
        );
    });

    morayClient.emitter.once('deleteMany', function _findObjects(
        bucket,
        filter,
        callback
    ) {
        var deleteIds;

        t.equal(bucket, MORAY_BUCKET, 'should be deleting from correct bucket');
        // LDAP is the worst
        t.ok(
            filter.match(/^\(\|(\(_id=[0-9]+\))+\)$/),
            'should delete based on an LDAP filter'
        );
        deleteIds = filter
            .replace(/[^0-9]/g, ' ')
            .trim()
            .split(/\s*/)
            .map(function(_id) {
                return Number(_id);
            });

        ids.sort();
        deleteIds.sort();

        t.deepEqual(deleteIds, ids, 'should have deleted all the ids');

        callback();
    });

    morayClient.emitter.once('findObjects', function _findObjects(obj) {
        vasync.whilst(
            function _checkDone() {
                return numObjectsReturned < numObjects;
            },
            function _addRecord(cb) {
                var recObj;
                var shark1 = Math.floor(Math.random() * 10);
                var shark2 = shark1;

                // ensure sharks are different
                while (shark2 === shark1) {
                    shark2 = Math.floor(Math.random() * 10);
                }

                expectedFiles[shark1] = (expectedFiles[shark1] || 0) + 1;
                expectedFiles[shark2] = (expectedFiles[shark2] || 0) + 1;

                ids.push(numObjectsReturned);

                recObj = _createRecord({
                    id: numObjectsReturned,
                    key: '/' + ownerId + '/stor/hello.txt',
                    sharks: [shark1, shark2]
                });

                setTimeout(function _endReq() {
                    numObjectsReturned++;
                    obj.req.emitter.emit('record', recObj);
                    cb();
                }, 5);
            },
            function _whilstDone() {
                // All done, send an end
                setTimeout(function _endReq() {
                    obj.req.emitter.emit('end');
                }, 10);
            }
        );
    });
});

test('stop GarbageDirConsumer', function _testStopDirConsumer(t) {
    morayClient.emitter.removeAllListeners();
    consumer.stop(function _onStop(err) {
        t.error(err, 'stop GarbageDirConsumer');
        t.end();
    });
});

test('delete testdirs', function _testDeleteTestdirs(t) {
    var disabled = Boolean(process.env.NO_DELETE);

    t.ok(
        true,
        'delete of testdirs (env NO_DELETE is' +
            (!disabled ? ' not' : '') +
            ' set)',
        {skip: disabled}
    );

    if (!disabled) {
        t.doesNotThrow(function _callDeleter() {
            _deleteTestDirs(t);
        }, 'delete test directories');
    }
    t.end();
});

test('dump logs', function _testDumpLogs(t) {
    var enabled = Boolean(process.env.DUMP_LOGS);

    t.ok(
        true,
        'log dump (env DUMP_LOGS is' + (!enabled ? ' not' : '') + ' set)',
        {skip: !enabled}
    );

    if (process.env.DUMP_LOGS) {
        console.dir(logs);
    }

    t.end();
});
