/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2019 Joyent, Inc.
 */

//
// This program exists to consume items from buckets-mdapi's garbage tables using the
// interfaces added with MANTA-4320.
//
// This means calling the `getgcbatch` RPC and writing instructions out to
// local "instructions" files in:
//
//  * /var/spool/manta_gc/<storageId>/*
//
// where they will be picked up and transferred to the storage zones for
// processing. Once records are written out locally, they will be removed from
// bbuckets-mdapi using the `deletegcbatch` RPC.
//
// The Instructions files have:
//
//  * storageId
//  * objectId
//  * creatorId/ownerId
//  * shardId
//  * bytes (for metrics/auditing)
//
// and therefore contain everything that's necessary to actually collect garbage
// on the individual storage/mako zones ("sharks").
//
// Config Options:
//
//  * bbuckets-mdapi shard(s) info
//
//
// TODO:
//
//  * Need tooling to assign bbuckets-mdapi shards to GC
//  * Tests
//

var fs = require('fs');
var util = require('util');

var assert = require('assert-plus');
var bucketsMdapi = require('boray');
var createMetricsManager = require('triton-metrics').createMetricsManager;
var restify = require('restify');
var uuid = require('uuid/v4');
var vasync = require('vasync');

var common = require('../lib/common');

var elapsedSince = common.elapsedSince;
var ensureDelegated = common.ensureDelegated;
var writeGarbageInstructions = common.writeGarbageInstructions;
var METRICS_SERVER_PORT = 8881;
var SERVICE_NAME = 'garbage-buckets-consumer';


function GarbageBucketsConsumer(opts) {
    var self = this;

    // XXX assert params

    var bucketsMdapiConfig;

    self.config = opts.config;
    self.deleteQueue = {};
    self.log = opts.log;
    self.metricsManager = opts.metricsManager;
    self.bucketsMdapiConfig = opts.bucketsMdapiConfig;
    self.nextRunTimer = null;
    self.shard = opts.shard;
    self.stats = {
        deletedByShard: {}, // XXX
        readByShard: {}     // XXX
    };

    //
    self.readBatchDelay = self.config.options.record_read_batch_delay || 13;
    self.readBatchSize = self.config.options.record_read_batch_size || 666;
}


GarbageBucketsConsumer.prototype.start = function start() {
    var self = this;

    var beginning = process.hrtime();

    self.log.info('Starting');

    self.client = bucketsMdapi.createClient(self.bucketsMdapiConfig);

    // XXX docs seem to say we shouldn't look at .on('error')?

    //
    // Unless we use the 'failFast' to the buckets-mdapi client, there is no
    // error emitted. Only 'connect' once it's actually connected.
    //
    self.client.once('connect', function () {
        var delay;

        self.log.info({
            elapsed: elapsedSince(beginning)
        }, 'Connected to buckets-mdapi.');

        // Pick a random offset between 0 and self.readBatchDelay for the first
        // run, in attempt to prevent all shards running at the same time.
        delay = Math.floor(Math.random() * self.readBatchDelay);

        self.log.info('Startup Complete. Will delay %d seconds before first read.', delay);

        self.nextRunTimer = setTimeout(function _firstRun() {
            self.run();
        }, delay * 1000);
    });
};


GarbageBucketsConsumer.prototype.stop = function stop() {
    var self = this;

    self.log.info('Stopping');

    if (self.nextRunTimer !== null) {
        clearTimeout(self.nextRunTimer);
        self.nextRunTimer = null;
    }
};


GarbageBucketsConsumer.prototype.run = function run() {
    var self = this;

    var beginning = process.hrtime();
    var reqId = uuid();

    self.log.info('Running Consumer.');

    vasync.pipeline({
        arg: {},
        funcs: [
            function _readBatch(ctx, cb) {
                var readBegin = process.hrtime();

                self.readGarbageBatch({
                    reqId: reqId
                }, function _onRead(err, results) {
                    self.log.info({
                        elapsed: elapsedSince(readBegin),
                        err: err,
                        results: results,
                        reqId: reqId
                    }, 'Finished reading garbage batch.');
                    ctx.results = results;
                    cb(err);
                });
            }, function _writeBatch(ctx, cb) {
                var writeBegin = process.hrtime();

                if (Object.keys(ctx.results.records).length === 0) {
                    self.log.info({
                        elapsed: elapsedSince(writeBegin),
                        reqId: reqId
                    }, 'No garbage records, skipping write.');
                    return;
                }

                writeGarbageInstructions(self, {
                    records: ctx.results.records,
                    reqId: reqId
                }, function _onWrite(err, results) {
                    self.log.info({
                        elapsed: elapsedSince(writeBegin),
                        err: err,
                        results: results,
                        reqId: reqId
                    }, 'Finished writing garbage instructions.');
                    cb();
                });
            }, function _deleteBatch(ctx, cb) {
                var deleteBegin = process.hrtime();

                if (Object.keys(ctx.results.records).length === 0) {
                    self.log.info({
                        elapsed: elapsedSince(deleteBegin),
                        reqId: reqId
                    }, 'No garbage records, skipping delete.');
                    return;
                }

                self.deleteProcessedGarbage({
                    batchId: ctx.results.batchId,
                    reqId: reqId
                }, function _onDelete(err, results) {
                    self.log.info({
                        elapsed: elapsedSince(deleteBegin),
                        err: err,
                        results: results,
                        reqId: reqId
                    }, 'Finished deleting garbage from buckets-mdapi.');
                    cb();
                });
            }
        ]
    }, function _onPipeline(err) {

        self.log.info({
            elapsed: elapsedSince(beginning),
            err: err,
            reqId: reqId
        }, 'Run complete.');

        if (err) {
            self.log.error({err: err}, 'Had an error.');
            // XXX do we need to clear the results?
        }

        // Schedule next run.
        self.log.info('Will run again in %d seconds.', self.readBatchDelay);

        self.nextRunTimer = setTimeout(function _nextRun() {
            self.run();
        }, self.readBatchDelay * 1000);
    });
};


GarbageBucketsConsumer.prototype.readGarbageBatch = function readGarbageBatch(opts, callback) {
    var self = this;

    assert.object(opts, 'opts');
    assert.uuid(opts.reqId, 'opts.reqId');
    assert.func(callback, 'callback');

    var beginning = process.hrtime();
    var counters = {
        totalBytes: 0,
        totalBytesWithCopies: 0,
        totalObjects: 0,
        totalObjectsByCopies: {},
        totalObjectsWithCopies: 0
    };
    var findOpts = {};
    var ids = [];
    var records = {};
    var req;
    var timeToFirstRecord;

    self.log.error('WOULD TRY GARBAGE BATCH');

    // TODO: assert that records is empty? We shouldn't have any because either
    // we just started, or previous loop should have cleared.
    // What happens on 'error'? Can we have some that were read before we got
    // the error?


    //
    // XXX eventually we should really support a garbage batch size parameter
    //     but that requires support in buckets-mdapi.
    //
    self.client.getGarbageBatch({request_id: opts.reqId}, function _onBatch(err, garbageBatch) {
        var idx;
        var garbageRecord;
        var sharkIdx;
        var storageId;

        if (err) {
            // XXX With Moray we'd check here whether it's overloaded.
            //     buckets-mdapi doesn't support that.
            self.log.error({
                elapsed: elapsedSince(beginning),
                err: err,
                timeToFirstRecord: timeToFirstRecord
            }, 'Error reading garbage batch.');

            callback(err);
            return;
        }

        self.log.trace({
            garbageBatch: garbageBatch,
            reqId: opts.reqId
        }, 'Got garbageBatch from buckets-mdapi.');

        // TODO: is there a way to get a count of remaining?

        if (timeToFirstRecord === undefined) {
            timeToFirstRecord = elapsedSince(beginning);
        }

        for (idx = 0; idx < garbageBatch.garbage.length; idx++) {
            garbageRecord = garbageBatch.garbage[idx];

            // TODO: validate garbageRecord

            counters.totalBytes += garbageRecord.content_length;
            counters.totalObjects++;

            if (counters.totalObjectsByCopies[garbageRecord.sharks.length] === undefined) {
                counters.totalObjectsByCopies[garbageRecord.sharks.length] = 0;
            }
            counters.totalObjectsByCopies[garbageRecord.sharks.length]++;

            for (sharkIdx = 0; sharkIdx < garbageRecord.sharks.length; sharkIdx++) {
                // XXX does buckets not track owner and creator separately?
                storageId = garbageRecord.sharks[sharkIdx].manta_storage_id;

                if (!records.hasOwnProperty(storageId)) {
                    records[storageId] = [];
                }

                records[storageId].push({
                    bytes: garbageRecord.content_length,
                    objectId: garbageRecord.id,
                    ownerId: garbageRecord.owner,
                    path: garbageRecord.name,
                    shard: self.shard,
                    storageId: storageId
                });

                counters.totalBytesWithCopies += garbageRecord.content_length;
                counters.totalObjectsWithCopies++;
            }
        }

        self.log.debug({
            elapsed: elapsedSince(beginning),
            timeToFirstRecord: timeToFirstRecord
        }, 'Done reading garbage batch.');

         callback(null, {
             batchId: garbageBatch.batch_id,
             records: records
         });
    });
};


GarbageBucketsConsumer.prototype.deleteProcessedGarbage = function deleteProcessedGarbage(opts, callback) {
    var self = this;

    var beginning = process.hrtime();
    var idx;

    assert.object(opts, 'opts');
    assert.uuid(opts.batchId, 'opts.batchId');
    assert.uuid(opts.reqId, 'opts.reqId');

    // XXX is there any case where we'll not have a batchId?

    // XXX DEBUG?
    self.log.info({opts: opts}, 'Deleting processed garbage.');

    self.client.deleteGarbageBatch({
        batch_id: opts.batchId,
        request_id: opts.reqId
    }, function _onDeleteBatch(err, deleteBatchResult) {
        if (err) {
            // TODO:  metric here?

            // XXX With Moray we'd check here whether it's overloaded.
            //     buckets-mdapi doesn't support that.
            self.log.error({
                batchId: opts.batchId,
                elapsed: elapsedSince(beginning),
                err: err,
                reqId: opts.reqId
            }, 'Error deleting garbage batch.');

            callback(err);
            return;
        }

        self.log.debug({
            batchId: opts.batchId,
            deleteBatchResult: deleteBatchResult,
            elapsed: elapsedSince(beginning),
            reqId: opts.reqId
        }, 'Called deleteGarbageBatch.');

        if (deleteBatchResult !== 'ok') {
            self.log.error({
                batchId: opts.batchId,
                deleteBatchResult: deleteBatchResult,
                elapsed: elapsedSince(beginning),
                reqId: opts.reqId
            }, 'Unexpected deleteBatchResult.');

            callback(new Error('Unexpected deleteBatchResult: ' +
                JSON.stringify(deleteBatchResult) + '.'));
            return;
        }

        callback();
    });
};


function main() {
    var beginning;
    var logger;

    beginning = process.hrtime();

    vasync.pipeline({
        arg: {},
        funcs: [
            function _createLogger(_, cb) {
                logger = common.createLogger({
                    level: 'trace', // XXX temporary
                    name: SERVICE_NAME
                });

                cb();
            }, function _loadConfig(ctx, cb) {
                common.loadConfig({log: logger}, function _onConfig(err, cfg) {
                    if (!err) {
                        ctx.config = cfg;
                    }
                    cb(err);
                });
            }, function _validateConfig(ctx, cb) {
                common.validateConfig(ctx.config, function _onValidated(err, res) {
                    cb(err);
                });
            }, function _ensureDelegated(_, cb) {
                //
                // If we don't have a delegated dataset, we're not going to do
                // anything else since it would be dangerous to write any files
                // locally.
                //
                ensureDelegated(function _delegatedResult(err, found) {
                    logger.debug({
                        err: err,
                        found: found
                    }, 'ensureDelegated result.');

                    if (err) {
                        cb(err);
                    } else if (!found) {
                        cb(new Error('Instruction root not on delegated ' +
                            'dataset, unsafe to continue.'));
                    } else {
                        cb();
                    }
                });
            }, function _setupMetrics(ctx, cb) {
                var metricsManager = createMetricsManager({
                    address: ctx.config.admin_ip,
                    log: logger,
                    staticLabels: {
                        datacenter: ctx.config.datacenter,
                        instance: ctx.config.instance,
                        server: ctx.config.server_uuid,
                        service: SERVICE_NAME
                    },
                    port: METRICS_SERVER_PORT,
                    restify: restify
                });
                metricsManager.createNodejsMetrics();

                // TODO: setup other metrics

                metricsManager.listen(cb);

                ctx.metricsManager = metricsManager;
            }, function _createBucketsConsumer(ctx, cb) {
                var childLog;
                var idx;
                var gdc;
                var shard;

                if (ctx.config.buckets_shards.length < 1) {
                    cb(new Error('No buckets shards configured for GC.'));
                    return;
                }

                //
                // We create a separate GarbageBucketsConsumer instance for each
                // buckets-mdapi shard this GC is assigned to manage.
                //
                for (idx = 0; idx < ctx.config.buckets_shards.length; idx++) {
                    shard = ctx.config.buckets_shards[idx].host;

                    childLog = logger.child({
                        component: 'GarbageBucketsConsumer',
                        shard: shard
                    }),

                    gdc = new GarbageBucketsConsumer({
                        config: ctx.config,
                        log: childLog,
                        metricsManager: ctx.metricsManager,
                        bucketsMdapiConfig: common.getBucketsMdapiConfig({
                            collector: ctx.metricsManager.collector,
                            config: ctx.config,
                            log: childLog,
                            bucketsMdapiShard: shard
                        }),
                        shard: shard
                    });

                    gdc.start();
                }

                cb();
            }
        ]
    }, function _doneMain(err) {
        logger.info({
            elapsed: elapsedSince(beginning),
            err: err
        }, 'Startup complete.');
    });
}


main();
