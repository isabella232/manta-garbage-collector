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

var GarbageBucketsConsumer = require('../lib/garbage-buckets-consumer.js');

var DATACENTER = 'trashland';
var MDAPI_SHARD = '1.buckets-mdapi.test.joyent.us';
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
var mdapiClient;
var runEmitter = new EventEmitter();
var test_dirs_created = false;

function _runHook(obj) {
    runEmitter.emit('run', obj);
}

//
// For obj:
//
//  - .ownerId should be a uuid
//  - .sharks should be array of numbers, e.g.: [1, 2]
//
function _createRecord(obj) {
    assert.object(obj, 'obj');
    assert.uuid(obj.ownerId, 'obj.ownerId');
    assert.array(obj.sharks, 'obj.sharks');

    var bucketId = uuid();
    var objectId = uuid();
    var ownerId = obj.ownerId;
    var record = {
        bucket_id: bucketId,
        content_length: Math.floor(Math.random() * 1000000),
        content_md5: crypto
            .createHash('md5')
            .update(objectId)
            .digest('base64'),
        content_type: 'application/octet-stream',
        created: new Date(Date.now() - 1000).toISOString(),
        headers: {},
        modified: new Date(Date.now() - 500).toISOString(),
        name: 'hello.txt',
        owner: ownerId,
        properties: {}
    };

    record.id = objectId;
    record.sharks = obj.sharks.map(function _sharkJumper(sId) {
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
        var pathFields;

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
                    fields[0],
                    expectedStorId,
                    'should have storId as fields[0]'
                );

                t.equal('DELETEv2', fields[1], '2nd field should be DELETEv2');

                pathFields = fields[2].split('/');

                //
                // fields[2] looks like:
                //
                // '/v2/552749fc-7ba4-41de-ae1f-c9b04bb40e88/ebfa7492-a8de-4028-8abb-f1fed922aa2d/91/917355dc-ef32-4485-b9e4-75dead3c1575,2e54144ba487ae25d03a3caba233da71'
                //
                // which looks like this when split on '/':
                //
                // [
                //   '',
                //   'v2',
                //   '552749fc-7ba4-41de-ae1f-c9b04bb40e88',
                //   'ebfa7492-a8de-4028-8abb-f1fed922aa2d',
                //   '91',
                //   '917355dc-ef32-4485-b9e4-75dead3c1575,2e54144ba487ae25d03a3caba233da71'
                // ]
                //
                // eslint-disable-next-line no-loop-func
                t.doesNotThrow(function _check3rdField() {
                    assert.equal(pathFields[0], '', 'should be absolute path');
                    assert.equal(pathFields[1], 'v2', 'should be v2');
                    assert.uuid(pathFields[2], 'owner should be a uuid');
                    assert.uuid(pathFields[3], 'bucket should be a uuid');
                    assert.equal(
                        pathFields[4].substr(0, 2),
                        pathFields[5].substr(0, 2),
                        'subdir should be prefix of final field'
                    );
                    assert.uuid(
                        pathFields[5].split(',')[0],
                        'first chunk of pathField[5] should be uuid'
                    );
                    assert.ok(
                        pathFields[5].split(',')[1].match(/^[a-f0-9]+$/),
                        'second chunk of pathField[5] should be hex string'
                    );
                    assert.equal(
                        pathFields[5].split(',').length,
                        2,
                        'should be only 2 fields in path'
                    );
                }, '3rd field should be valid, found: [' + fields[2] + ']');

                t.equal(
                    fields[3],
                    MDAPI_SHARD,
                    'fields[3] should be our mdapi shard'
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
// A dummy mdapi client. This one we can make return what we want.
//
// eslint-disable-next-line no-unused-vars
function DummyMdapiClient(opts) {
    var self = this;

    self.emitter = new EventEmitter();
}

DummyMdapiClient.prototype.once = function clientOnce(evt, callback) {
    if (evt === 'connect') {
        // Just wait 10ms and pretend we connected
        setTimeout(callback, 10);
        return;
    }

    throw new Error('BOOM');
};

DummyMdapiClient.prototype.getGCBatch = function getGCBatch(reqId, callback) {
    var self = this;

    var garbageBatch = {
        batch_id: null,
        garbage: []
    };

    if (self.emitter.listeners('getGCBatch').length > 0) {
        // If there's a listener, they need to call the callback
        self.emitter.emit('getGCBatch', reqId, callback);
    } else {
        callback(null, garbageBatch);
    }
};

DummyMdapiClient.prototype.deleteGCBatch = function deleteGCBatch(
    batchId,
    reqId,
    callback
) {
    var self = this;

    var deleteBatchResult = 'ok';

    if (self.emitter.listeners('deleteGCBatch').length > 0) {
        // If there's a listener, they need to call the callback
        self.emitter.emit('deleteGCBatch', batchId, reqId, callback);
    } else {
        callback(null, deleteBatchResult);
    }
};

test('create testdirs', function _testCreateTestdirs(t) {
    t.doesNotThrow(function _callCreator() {
        _createTestDirs(t);
    }, 'create test directories');
    t.end();
});

test('create GarbageBucketsConsumer', function _testCreateBucketsConsumer(t) {
    mdapiClient = new DummyMdapiClient({shard: MDAPI_SHARD});

    consumer = new GarbageBucketsConsumer({
        config: {
            instance: uuid(),
            options: {
                record_read_batch_size: TEST_READ_BATCH_SIZE,
                record_read_wait_interval: 1
            }
        },
        instructionRoot: TEST_DIR_INSTRUCTIONS,
        log: logger,
        _mdapiClient: mdapiClient,
        bucketsMdapiConfig: {},
        _runHook: _runHook,
        shard: MDAPI_SHARD
    });

    t.ok(consumer, 'create GarbageBucketsConsumer');

    // consumer.start() doesn't call the callback until the first .run()
    // completes, so we need to make sure here that we handle the findObjects
    // and return 'end' so that it looks like it just found nothing and the run
    // completes.
    mdapiClient.emitter.once('findObjects', function _findObjects(obj) {
        t.ok(true, 'saw findObjects');

        setTimeout(function _endReq() {
            obj.req.emitter.emit('end');
        }, 10);
    });

    // finish the test once the first run is complete.
    runEmitter.once('run', function _onRun(_obj) {
        t.end();
    });

    consumer.start(function _started(err) {
        t.ifError(err, 'start GarbageBucketsConsumer');
    });
});

//
// getGCBatch requests should be happening on each run and should be emitted on
// the consumer.emitter's 'getGCBatch' channel. So here we just grab the next
// one of these events check it. Since the only argument is a reqId, there's not
// really that much to check.
//
test('check getGCBatch requests', function _testGetGCBatch(t) {
    // The run should complete after we reply to the getGCBatch()
    runEmitter.once('run', function _onRun(_obj) {
        t.ifError(_obj.err, 'run: should be no error');
        t.equal(_obj.deletes, 0, 'run: should be no deletes');
        t.equal(_obj.reads, 0, 'run: should be no reads');
        t.equal(_obj.writes, 0, 'run: should be no reads');

        t.end();
    });

    mdapiClient.emitter.once('getGCBatch', function _getGCBatch(
        reqId,
        callback
    ) {
        t.doesNotThrow(function _checkReqId() {
            assert.uuid(reqId, 'reqId');
        }, 'reqId should be valid UUID');

        // This is what mdapi sends for an empty result
        callback(null, {
            batch_id: null,
            garbage: []
        });
    });
});

test('check getGCBatch requests with results', function _testGetGCBatchRequestsResults(t) {
    var batchId = uuid();
    var expectedFiles = {};
    var numObjects = 10;
    var ownerId = uuid();

    // The run will complete after the deletion is complete
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

    mdapiClient.emitter.once('deleteGCBatch', function _deleteGCBatch(
        _batchId,
        reqId,
        cb
    ) {
        t.equal(
            _batchId,
            batchId,
            'deleteGCBatch batchId should match value returned from getGCBatch'
        );
        t.doesNotThrow(function _checkIds() {
            assert.uuid(reqId, 'reqId');
        }, 'reqId should be a UUID');

        // There's not much to this interface that we can test.

        cb(null, 'ok');
    });

    mdapiClient.emitter.once('getGCBatch', function _getGCBatch(reqId, cb) {
        var garbageBatch = {
            batch_id: batchId,
            garbage: []
        };
        var idx;
        var shark1;
        var shark2;

        for (idx = 0; idx < numObjects; idx++) {
            shark1 = Math.floor(Math.random() * 10);
            shark2 = shark1;

            // ensure sharks are different
            while (shark2 === shark1) {
                shark2 = Math.floor(Math.random() * 10);
            }

            expectedFiles[shark1] = (expectedFiles[shark1] || 0) + 1;
            expectedFiles[shark2] = (expectedFiles[shark2] || 0) + 1;

            garbageBatch.garbage.push(
                _createRecord({
                    ownerId: ownerId,
                    sharks: [shark1, shark2]
                })
            );
        }

        // Return this garbage batch to the getGCBatch caller.
        cb(null, garbageBatch);
    });
});

test('stop GarbageBucketsConsumer', function _testStopBucketsConsumer(t) {
    mdapiClient.emitter.removeAllListeners();
    consumer.stop(function _onStop(err) {
        t.error(err, 'stop GarbageBucketsConsumer');
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
