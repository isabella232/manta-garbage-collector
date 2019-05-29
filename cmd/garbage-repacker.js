/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2019, Joyent, Inc.
 */

//
// This program exists to consume items from the:
//
//  * manta_fastdelete_queue
//
// bucket(s), and repack them into separate per-"shark" records in the
// "manta_garbage" bucket. These separated records include:
//
//  * bytes (for metrics/auditing)
//  * objectId
//  * ownerId
//  * path (for debugging)
//  * storageId
//
// and therefore contain everything that's necessary to actually collect garbage
// on the individual storage/mako zones ("sharks"). Because they're separated
// into individual required delete actions, the records here can be removed as
// another process sweeps through the "manta_garbage" table and sends the
// instructions to the appropriate storage zones.
//
// Note: this program is only required because the existing deletion records are
// in an unfortunate form that includes the sharks as an array. If we can
// eventually modify muskie + the triggers to insert separate rows for each
// deletion that's required, we could do away with this program entirely.
//


// Things to think about:
//
//  * do we need to worry about _id wraparound? Probably not since we'll
//    eventually consume everything?
//

var fs = require('fs');
var path = require('path');

var assert = require('assert-plus');
var bunyan = require('bunyan');
var createMetricsManager = require('triton-metrics').createMetricsManager;
var jsprim = require('jsprim');
var moray = require('moray');
var vasync = require('vasync');
var verror = require('verror');
var VError = verror.VError;

// XXX rename this common before I die
var common = require('../lib/common.new');


function GarbageRepacker(opts) {
    var self = this;

    // XXX assert params

    var morayConfig;

    self.shard = opts.shard.host; // XXX
    self.config = opts.config;
    self.log = opts.log.child({
        component: 'GarbageRepacker',
        shard: self.shard
    });

    self.log.info({opts: opts}, 'new GarbageRepacker');

    self.lastRepack = 0;

    self.morayConfig = common.getMorayConfig({
        collector: undefined, // XXX replace with collector
        config: self.config,
        log: self.log,
        morayShard: self.shard
    });

    // XXX error?!
    self.log.error({morayConfig: self.morayConfig}, 'moray config');
}

GarbageRepacker.prototype.repack = function repack() {
    var self = this;

    var beginning = process.hrtime();
    var timers = {};

    self.log.info('repacking');
    self.lastRepack = Date.now();

    vasync.pipeline({
        arg: {},
        funcs: [
            function _findLoad(ctx, cb) {
                findGarbageLoad({
                    log: self.log,
                    morayClient: self.client
                }, function _onGarbageLoad(err, garbageLoad) {
                    if (!err) {
                        ctx.load = garbageLoad;
                    }

                    timers.findGarbageLoad = common.elapsedSince(beginning);
                    cb(err);
                });
            }, function _repackLoad(ctx, cb) {
                repackGarbageLoad({
                    log: self.log,
                    morayClient: self.client
                }, ctx.load, function _onGarbageLoadRepacked(err) {
                    timers.repackGarbageLoad =
                        common.elapsedSince(beginning, timers.findGarbageLoad);
                    cb(err);
                });
            }
        ]
    }, function _repacked(err) {
        self.log.info({
            err: err,
            timers: timers
        }, 'repacked!');

        // XXX if the last load was a "full load" do we want to schedule sooner?
        //     and if the last load was "empty" do we schedule later?
        self.scheduleNextRepack();
    });
};

GarbageRepacker.prototype.scheduleNextRepack = function scheduleNextRepack() {
    var self = this;

    if (self.lastRepack === 0) {
        self.repack();
    } else {
        setTimeout(self.repack.bind(self), 10000);
    }
};

GarbageRepacker.prototype.start = function start(callback) {
    var self = this;

    self.log.info('starting');

    vasync.pipeline({
        arg: {},
        funcs: [
            function _connectMoray(_, cb) {
                self.client = moray.createClient(self.morayConfig);

                // XXX on error, do again after delay?

                self.client.once('connect', function () {
                    self.log.info('connected');
                    cb();
                });
            }, function _createBucket(_, cb) {
                createMantaGarbageBucket({
                    log: self.log,
                    morayClient: self.client
                }, function _onCreateBucket(err) {
                    cb(err);
                });
            }, function _scheduleFirstRepack(ctx, cb) {
                self.scheduleNextRepack();
                cb();
            }
        ]
    }, function _started(err) {
        self.log.info({err: err}, 'started!');
    });
};

function createMantaGarbageBucket(opts, callback) {
    var log = opts.log;
    var morayClient = opts.morayClient;

    morayClient.getBucket(common.MANTA_GARBAGE_BUCKET_NAME,
        function _onBucket(err, bucketInfo) {

        log.info({
            bucketInfo: bucketInfo,
            err: err
        }, 'bucket info');

        if (err) {
            if (VError.hasCauseWithName(err, 'BucketNotFoundError')) {
                log.info('bucket does not exist, will create');
                morayClient.createBucket(common.MANTA_GARBAGE_BUCKET_NAME,
                    common.MANTA_GARBAGE_BUCKET,
                    function _onCreated(createdErr, createdFoo) {

                    log.info({
                        err: createdErr,
                        foo: createdFoo
                    }, 'created?');
                    callback();
                });
            } else {
                callback(err);
            }
            // XXX if error getting or creating, try again
        } else {
            log.info({bucketInfo: bucketInfo}, 'already have bucket');
            callback();
        }
    });
}

// TODO: add a timer
function findGarbageLoad(opts, callback) {
    var counters = {
        totalBytes: 0,
        totalBytesWithCopies: 0,
        totalObjects: 0,
        totalObjectsByCopies: {},
        totalObjectsWithCopies: 0
    };
    var findOpts = {};
    var log = opts.log;
    var maxId = 0;
    var morayClient = opts.morayClient;
    var records = [];
    var req;

    findOpts.limit = 1000; // XXX batch size
    findOpts.sort = {
        attribute: '_id',
        order: 'ASC'
    };

    //
    // XXX do we eventually need to check multiple buckets (e.g. something for
    // "manta buckets" and the manta_delete_log)?
    //
    req = morayClient.findObjects(common.FASTDELETE_BUCKET, '(_id>=0)', findOpts);

    req.once('error', function (err) {
        log.info({err: err}, 'error');
        callback(err);
    });

    req.on('record', function (obj) {
        var idx;
        var record;
        var value = obj.value;

        // XXX verify obj is a good obj
        //     assert value.type === 'object'?

        obj.value;

        if (obj._id >= maxId) {
            maxId = obj._id;
        } else {
            log.error({oldId: maxId, newId: obj._id}, '_id went backward?!');
        }

        counters.totalBytes += value.contentLength;
        counters.totalObjects++;

        // NOTE: 0-byte objects will have no "sharks", these will also get
        // pruned out here (since the for loop won't include them in records)
        // but we still want to count them for metrics purposes.

        if (counters.totalObjectsByCopies[value.sharks.length] === undefined) {
            counters.totalObjectsByCopies[value.sharks.length] = 0;
        }
        counters.totalObjectsByCopies[value.sharks.length]++;

        for (idx = 0; idx < value.sharks.length; idx++) {
            records.push({
                bytes: value.contentLength,
                objectId: value.objectId,
                ownerId: value.owner,
                path: value.key,
                storageId: value.sharks[idx].manta_storage_id
            });
            counters.totalBytesWithCopies += value.contentLength;
            counters.totalObjectsWithCopies++;
        }
    });

    req.on('end', function () {
        log.info({
            counters: counters,
            maxId: maxId,
            records: records.length,
            source: 'manta_fastdelete_queue'
        }, 'end');

        callback(null, {
            counters: counters,
            maxId: maxId,
            records: records,
            source: 'manta_fastdelete_queue'
        });
    });
}

// TODO: add a timer
function repackGarbageLoad(opts, load, callback) {
    // XXX TODO validate params
    var batch = [];
    var idx = 0;
    var log = opts.log;
    var morayClient = opts.morayClient;
    var record;

    if (load.records.length === 0) {
        log.info('no records found, skipping repack');
        callback();
        return;
    }

    for (idx = 0; idx < load.records.length; idx++) {
        record = load.records[idx];

        batch.push({
            bucket: common.MANTA_GARBAGE_BUCKET_NAME,
            key: record.objectId + '/' + record.storageId,
            value: record
        });
    }

    batch.push({
        bucket: load.source,
        operation: 'deleteMany',
        filter: '(_id<=' + load.maxId + ')'
    });

    // XXX could add req_id to options here...
    morayClient.batch(batch, {}, function _onBatch(err, metadata) {
        log.warn({
            err: err,
            metadata: metadata
        }, 'did batch');

        callback(err);
    });
}

// TODO setup metrics

function main() {
    var logger;

    vasync.pipeline({
        arg: {},
        funcs: [
            function _createLogger(ctx, cb) {
                logger = ctx.log = common.createLogger({
                    name: 'garbage-repacker'
                });

                cb();
            }, function _loadConfig(ctx, cb) {
                common.loadConfig({log: ctx.log}, function _onConfig(err, cfg) {
                    if (!err) {
                        ctx.config = cfg;
                    }
                    cb(err);
                });
            }, function _validateConfig(ctx, cb) {
                ctx.log.info({ctx: ctx}, 'would validate config');
                common.validateConfig(ctx.config, function _onValidated(err, res) {
                    // XXX wtf
                    cb(err);
                });
            }, function _createRepackers(ctx, cb) {
                var idx;
                var gs;

                for (idx = 0; idx < ctx.config.shards.length; idx++) {
                    gs = new GarbageRepacker({
                        config: ctx.config,
                        log: ctx.log,
                        shard: ctx.config.shards[idx]
                    });
                    gs.start();
                }

                cb();
            }
        ]
    }, function _doneMain(err) {
        logger.info('startup complete');
    });
}

main();
