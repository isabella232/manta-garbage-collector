/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

//
// This contains the code for GarbageBucketsConsumer objects which are used by
// the garbage-buckets-consumer program to pull garbage records from
// buckets-mdapi and create instructions files to send to storage servers.
//

var assert = require('assert-plus');
var bucketsMdapi = require('boray');
var uuid = require('uuid/v4');
var vasync = require('vasync');

var common = require('../lib/common');

var elapsedSince = common.elapsedSince;
var writeGarbageInstructions = common.writeGarbageInstructions;

function GarbageBucketsConsumer(opts) {
    var self = this;

    // XXX assert params

    self.config = opts.config;
    self.deleteQueue = {};
    self.log = opts.log;
    self.metricsManager = opts.metricsManager;
    self.bucketsMdapiConfig = opts.bucketsMdapiConfig;
    self.nextRunTimer = null;
    self.shard = opts.shard;
    self.stats = {
        deletedByShard: {}, // XXX
        readByShard: {} // XXX
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
    self.client.once('connect', function() {
        var delay;

        self.log.info(
            {
                elapsed: elapsedSince(beginning)
            },
            'Connected to buckets-mdapi.'
        );

        // Pick a random offset between 0 and self.readBatchDelay for the first
        // run, in attempt to prevent all shards running at the same time.
        delay = Math.floor(Math.random() * self.readBatchDelay);

        self.log.info(
            'Startup Complete. Will delay %d seconds before first read.',
            delay
        );

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

    vasync.pipeline(
        {
            arg: {},
            funcs: [
                function _readBatch(ctx, cb) {
                    var readBegin = process.hrtime();

                    self.readGarbageBatch(
                        {
                            reqId: reqId
                        },
                        function _onRead(err, results) {
                            self.log.info(
                                {
                                    elapsed: elapsedSince(readBegin),
                                    err: err,
                                    results: results,
                                    reqId: reqId
                                },
                                'Finished reading garbage batch.'
                            );
                            ctx.results = results;
                            cb(err);
                        }
                    );
                },
                function _writeBatch(ctx, cb) {
                    var writeBegin = process.hrtime();

                    if (Object.keys(ctx.results.records).length === 0) {
                        self.log.info(
                            {
                                elapsed: elapsedSince(writeBegin),
                                reqId: reqId
                            },
                            'No garbage records, skipping write.'
                        );
                        return;
                    }

                    writeGarbageInstructions(
                        self,
                        {
                            records: ctx.results.records,
                            reqId: reqId
                        },
                        function _onWrite(err, results) {
                            self.log.info(
                                {
                                    elapsed: elapsedSince(writeBegin),
                                    err: err,
                                    results: results,
                                    reqId: reqId
                                },
                                'Finished writing garbage instructions.'
                            );
                            cb();
                        }
                    );
                },
                function _deleteBatch(ctx, cb) {
                    var deleteBegin = process.hrtime();

                    if (Object.keys(ctx.results.records).length === 0) {
                        self.log.info(
                            {
                                elapsed: elapsedSince(deleteBegin),
                                reqId: reqId
                            },
                            'No garbage records, skipping delete.'
                        );
                        return;
                    }

                    self.deleteProcessedGarbage(
                        {
                            batchId: ctx.results.batchId,
                            reqId: reqId
                        },
                        function _onDelete(err, results) {
                            self.log.info(
                                {
                                    elapsed: elapsedSince(deleteBegin),
                                    err: err,
                                    results: results,
                                    reqId: reqId
                                },
                                'Finished deleting garbage from buckets-mdapi.'
                            );
                            cb();
                        }
                    );
                }
            ]
        },
        function _onPipeline(err) {
            self.log.info(
                {
                    elapsed: elapsedSince(beginning),
                    err: err,
                    reqId: reqId
                },
                'Run complete.'
            );

            if (err) {
                self.log.error({err: err}, 'Had an error.');
                // XXX do we need to clear the results?
            }

            // Schedule next run.
            self.log.info('Will run again in %d seconds.', self.readBatchDelay);

            self.nextRunTimer = setTimeout(function _nextRun() {
                self.run();
            }, self.readBatchDelay * 1000);
        }
    );
};

GarbageBucketsConsumer.prototype.readGarbageBatch = function readGarbageBatch(
    opts,
    callback
) {
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
    var records = {};
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
    self.client.getGarbageBatch({request_id: opts.reqId}, function _onBatch(
        err,
        garbageBatch
    ) {
        var idx;
        var garbageRecord;
        var sharkIdx;
        var storageId;

        if (err) {
            // XXX With Moray we'd check here whether it's overloaded.
            //     buckets-mdapi doesn't support that.
            self.log.error(
                {
                    elapsed: elapsedSince(beginning),
                    err: err,
                    timeToFirstRecord: timeToFirstRecord
                },
                'Error reading garbage batch.'
            );

            callback(err);
            return;
        }

        self.log.trace(
            {
                garbageBatch: garbageBatch,
                reqId: opts.reqId
            },
            'Got garbageBatch from buckets-mdapi.'
        );

        if (!garbageBatch.garbage) {
            self.log.error(
                {
                    garbageBatch: garbageBatch
                },
                'Bad garbage batch.'
            );

            callback(new Error('Bad garbage batch'));
            return;
        }

        // TODO: is there a way to get a count of remaining?

        if (timeToFirstRecord === undefined) {
            timeToFirstRecord = elapsedSince(beginning);
        }

        for (idx = 0; idx < garbageBatch.garbage.length; idx++) {
            garbageRecord = garbageBatch.garbage[idx];

            // TODO: validate garbageRecord

            counters.totalBytes += garbageRecord.content_length;
            counters.totalObjects++;

            if (
                counters.totalObjectsByCopies[garbageRecord.sharks.length] ===
                undefined
            ) {
                counters.totalObjectsByCopies[garbageRecord.sharks.length] = 0;
            }
            counters.totalObjectsByCopies[garbageRecord.sharks.length]++;

            for (
                sharkIdx = 0;
                sharkIdx < garbageRecord.sharks.length;
                sharkIdx++
            ) {
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

        self.log.debug(
            {
                elapsed: elapsedSince(beginning),
                timeToFirstRecord: timeToFirstRecord
            },
            'Done reading garbage batch.'
        );

        callback(null, {
            batchId: garbageBatch.batch_id,
            records: records
        });
    });
};

GarbageBucketsConsumer.prototype.deleteProcessedGarbage = function deleteProcessedGarbage(
    opts,
    callback
) {
    var self = this;

    var beginning = process.hrtime();

    assert.object(opts, 'opts');
    assert.uuid(opts.batchId, 'opts.batchId');
    assert.uuid(opts.reqId, 'opts.reqId');

    // XXX is there any case where we'll not have a batchId?

    // XXX DEBUG?
    self.log.info({opts: opts}, 'Deleting processed garbage.');

    self.client.deleteGarbageBatch(
        {
            batch_id: opts.batchId,
            request_id: opts.reqId
        },
        function _onDeleteBatch(err, deleteBatchResult) {
            if (err) {
                // TODO:  metric here?

                // XXX With Moray we'd check here whether it's overloaded.
                //     buckets-mdapi doesn't support that.
                self.log.error(
                    {
                        batchId: opts.batchId,
                        elapsed: elapsedSince(beginning),
                        err: err,
                        reqId: opts.reqId
                    },
                    'Error deleting garbage batch.'
                );

                callback(err);
                return;
            }

            self.log.debug(
                {
                    batchId: opts.batchId,
                    deleteBatchResult: deleteBatchResult,
                    elapsed: elapsedSince(beginning),
                    reqId: opts.reqId
                },
                'Called deleteGarbageBatch.'
            );

            if (deleteBatchResult !== 'ok') {
                self.log.error(
                    {
                        batchId: opts.batchId,
                        deleteBatchResult: deleteBatchResult,
                        elapsed: elapsedSince(beginning),
                        reqId: opts.reqId
                    },
                    'Unexpected deleteBatchResult.'
                );

                callback(
                    new Error(
                        'Unexpected deleteBatchResult: ' +
                            JSON.stringify(deleteBatchResult) +
                            '.'
                    )
                );
                return;
            }

            callback();
        }
    );
};

module.exports = GarbageBucketsConsumer;
