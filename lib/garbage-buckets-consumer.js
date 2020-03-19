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
// Future:
//
//  - Should we include shard label on metrics? Cardinality...
//

var crypto = require('crypto');

var assert = require('assert-plus');
var bucketsMdapi = require('buckets-mdapi');
var uuid = require('uuid/v4');
var vasync = require('vasync');
var VError = require('verror');

var common = require('../lib/common');

var elapsedSince = common.elapsedSince;
var writeGarbageInstructions = common.writeGarbageInstructions;

//
// As a safety we make sure that we don't have too many garbage records for
// mako to process. In the future it would be nice if mdapi supported passing a
// limit.
//
var MAX_GARBAGE_RECORDS = 1000;

function GarbageBucketsConsumer(opts) {
    var self = this;

    assert.object(opts, 'opts');
    assert.object(opts.config, 'opts.config');
    assert.optionalString(opts.instructionRoot, 'opts.instructionRoot');
    assert.object(opts.log, 'opts.log');
    assert.object(opts.bucketsMdapiConfig, 'opts.bucketsMdapiConfig');
    assert.optionalObject(opts._mdapiClient, 'opts._mdapiClient');
    assert.optionalString(opts.metricPrefix, 'opts.metricPrefix');
    assert.optionalObject(opts.metricsManager, 'opts.metricsManager');
    assert.optionalFunc(opts._runHook, 'opts._runHook');
    assert.string(opts.shard, 'opts.shard');

    var metricPrefix = opts.metricPrefix || '';

    // Options for testing
    self._runHook = opts._runHook;
    self.client = opts._mdapiClient; // allow override for testing
    self.instructionRoot = opts.instructionRoot;

    // Regular options
    self.config = opts.config;
    self.log = opts.log;
    self.metricsManager = opts.metricsManager;
    self.bucketsMdapiConfig = opts.bucketsMdapiConfig;
    self.nextRunTimer = null;
    self.shard = opts.shard;

    if (!self.config.options) {
        self.config.options = {};
    }

    //
    // buckets_batch_interval_ms sets the number of *milliseconds* we wait after
    // each run completes before we start the next one. Each run will do a
    // getGCBatch and then create instructions files from the results. The
    // default value of 1 minute was chosen without any real data and might need
    // to be adjusted based on real-world usage. This parameter can be set via
    // the SAPI configuration.
    //
    // NOTE: unlike the garbage-dir-consumer, there's no ability with mdapi to
    // set a limit on the number of records, so we have no control here.
    //
    self.readBatchDelay =
        self.config.options.buckets_batch_interval_ms || 60000;

    if (self.metricsManager) {
        self.metrics = {
            instrFileWriteCount: self.metricsManager.collector.counter({
                name: metricPrefix + 'file_write_count_total',
                help: 'Total number of instruction files written.'
            }),
            instrFileWriteErrorCount: self.metricsManager.collector.counter({
                name: metricPrefix + 'file_write_error_count_total',
                help: 'Total number of errors writing instruction files.'
            }),
            instrFileWriteSeconds: self.metricsManager.collector.counter({
                name: metricPrefix + 'file_write_seconds_total',
                help: 'Total time (in seconds) spent writing instruction files.'
            }),
            instrWriteCount: self.metricsManager.collector.counter({
                name: metricPrefix + 'instr_write_count_total',
                help: 'Total number of instruction files written.'
            }),
            mdapiBytesRead: self.metricsManager.collector.counter({
                name: metricPrefix + 'mdapi_record_bytes_count_total',
                help:
                    'Number of *object* bytes represented by garbage records ' +
                    'read by the dir-consumer from mdapi.'
            }),
            mdapiBytesReadWithCopies: self.metricsManager.collector.counter({
                name: metricPrefix + 'mdapi_record_copies_bytes_count_total',
                help:
                    'Number of *object* bytes represented by garbage records ' +
                    'read by the dir-consumer from mdapi (including copies).'
            }),
            mdapiRecordsRead: self.metricsManager.collector.counter({
                name: metricPrefix + 'mdapi_record_read_count_total',
                help:
                    'Number of mdapi garbage records read by the dir-consumer.'
            }),
            mdapiRecordsReadWithCopies: self.metricsManager.collector.counter({
                name: metricPrefix + 'mdapi_record_copies_read_count_total',
                help:
                    'Number of mdapi garbage records read by the dir-consumer' +
                    ' (including copies).'
            }),
            runCountTotal: self.metricsManager.collector.counter({
                name: metricPrefix + 'run_count_total',
                help:
                    'Counter incremented every time the dir-consumer looks ' +
                    'for new garbage in mdapi.'
            }),
            runErrorCountTotal: self.metricsManager.collector.counter({
                name: metricPrefix + 'run_error_count_total',
                help:
                    'Counter incremented every time the dir-consumer looks ' +
                    'for new garbage in mdapi and has an error.'
            }),
            runSecondsTotal: self.metricsManager.collector.counter({
                name: metricPrefix + 'run_seconds_total',
                help:
                    'Total number of seconds spent scanning mdapi and writing' +
                    ' instruction files.'
            }),
            mdapiBatchReadCount: self.metricsManager.collector.counter({
                name: metricPrefix + 'mdapi_batch_read_count_total',
                help: 'Total number of "batches" of records read from mdapi.'
            }),
            mdapiBatchReadErrorCount: self.metricsManager.collector.counter({
                name: metricPrefix + 'mdapi_batch_read_error_count_total',
                help:
                    'Total number of errors encountered  reading "batches" ' +
                    'of records read from mdapi.'
            }),
            mdapiBatchReadSeconds: self.metricsManager.collector.counter({
                name: metricPrefix + 'mdapi_batch_read_seconds_total',
                help:
                    'Total number of seconds spent reading batches of records' +
                    ' from mdapi.'
            }),
            mdapiBatchDeleteCount: self.metricsManager.collector.counter({
                name: metricPrefix + 'mdapi_batch_delete_count_total',
                help:
                    'Number of deleteMany calls to delete batches of records ' +
                    'from mdapi.'
            }),
            mdapiBatchDeleteErrorCount: self.metricsManager.collector.counter({
                name: metricPrefix + 'mdapi_batch_delete_error_count_total',
                help:
                    'Number of errors encountered deleting batches of records' +
                    ' from mdapi.'
            }),
            mdapiBatchDeleteSeconds: self.metricsManager.collector.counter({
                name: metricPrefix + 'mdapi_batch_delete_seconds_total',
                help:
                    'Total number of seconds spent deleting batches of records' +
                    ' from mdapi.'
            }),
            mdapiRecordDeleteCount: self.metricsManager.collector.counter({
                name: metricPrefix + 'mdapi_record_delete_count_total',
                help:
                    'Total number of processed garbage records deleted from ' +
                    'mdapi.'
            })
        };
    } else {
        self.metrics = {};
    }

    self.addCounter('mdapiBatchReadCount', 0);
    self.addCounter('mdapiBatchReadErrorCount', 0);
    self.addCounter('mdapiBatchReadSeconds', 0);

    self.addCounter('instrFileWriteCount', 0);
    self.addCounter('instrFileWriteErrorCount', 0);
    self.addCounter('instrFileWriteSeconds', 0);
    self.addCounter('instrWriteCount', 0);

    self.addCounter('mdapiBytesRead', 0);
    self.addCounter('mdapiBytesReadWithCopies', 0);
    self.addCounter('mdapiRecordsRead', 0);
    self.addCounter('mdapiRecordsReadWithCopies', 0);

    self.addCounter('runCountTotal', 0);
    self.addCounter('runErrorCountTotal', 0);
    self.addCounter('runSecondsTotal', 0);

    self.addCounter('mdapiBatchReadCount', 0);
    self.addCounter('mdapiBatchReadErrorCount', 0);
    self.addCounter('mdapiBatchReadSeconds', 0);

    self.addCounter('mdapiBatchDeleteCount', 0);
    self.addCounter('mdapiBatchDeleteErrorCount', 0);
    self.addCounter('mdapiBatchDeleteSeconds', 0);
    self.addCounter('mdapiRecordDeleteCount', 0);
}

GarbageBucketsConsumer.prototype.addCounter = common.addCounter;
GarbageBucketsConsumer.prototype.getCounter = common.getCounter;
GarbageBucketsConsumer.prototype.getGauge = common.getGauge;
GarbageBucketsConsumer.prototype.setGauge = common.setGauge;

GarbageBucketsConsumer.prototype.start = function start(callback) {
    var self = this;

    var beginning = process.hrtime();

    self.log.info('Starting GarbageBucketsConsumer');

    if (!self.client) {
        self.client = bucketsMdapi.createClient(self.bucketsMdapiConfig);
    }

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
            'Startup Complete. Will delay %d ms before first read.',
            delay
        );

        self.nextRunTimer = setTimeout(function _firstRun() {
            self.run(callback);
        }, delay);
    });
};

GarbageBucketsConsumer.prototype.stop = function stop(callback) {
    var self = this;

    self.log.info('Stopping GarbageBucketsConsumer');

    if (self.nextRunTimer !== null) {
        clearTimeout(self.nextRunTimer);
        self.nextRunTimer = null;
    }

    if (callback) {
        callback();
    }
};

GarbageBucketsConsumer.prototype.run = function run(callback) {
    var self = this;

    var beginning = process.hrtime();
    var deletedRecords = 0;
    var readRecords = 0;
    var reqId = uuid();
    var writtenRecords = 0;

    self.addCounter('runCountTotal', 1);
    self.log.debug('Running buckets consumer.');

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
                            self.addCounter('mdapiBatchReadCount', 1);
                            self.addCounter(
                                'mdapiBatchReadSeconds',
                                elapsedSince(readBegin)
                            );

                            if (err) {
                                self.addCounter('mdapiBatchReadErrorCount', 1);
                            } else {
                                readRecords = results.recordCount;
                            }
                            ctx.results = results;

                            self.log.debug(
                                {
                                    elapsed: elapsedSince(readBegin),
                                    err: err,
                                    results: results,
                                    reqId: reqId
                                },
                                'Finished reading garbage batch.'
                            );
                            cb(err);
                        }
                    );
                },
                function _writeBatch(ctx, cb) {
                    var writeBegin = process.hrtime();

                    if (Object.keys(ctx.results.records).length === 0) {
                        self.log.debug(
                            {
                                elapsed: elapsedSince(writeBegin),
                                reqId: reqId
                            },
                            'No garbage records, skipping write.'
                        );
                        cb();
                        return;
                    }

                    writeGarbageInstructions(
                        self,
                        {
                            instructionRoot: self.instructionRoot,
                            records: ctx.results.records,
                            reqId: reqId,
                            type: 'bucketsv2'
                        },
                        function _onWrite(err, results) {
                            self.addCounter(
                                'instrFileWriteCount',
                                results.filesWritten
                            );
                            self.addCounter(
                                'instrFileWriteSeconds',
                                elapsedSince(writeBegin)
                            );
                            self.addCounter(
                                'instrWriteCount',
                                results.instrsWritten
                            );
                            writtenRecords = results.instrsWritten;

                            if (err) {
                                self.addCounter('instrFileWriteErrorCount', 1);
                            }

                            self.log[err ? 'error' : 'debug'](
                                {
                                    elapsed: elapsedSince(writeBegin),
                                    err: err,
                                    results: results,
                                    reqId: reqId
                                },
                                'Finished writing garbage instructions.'
                            );

                            cb(err);
                        }
                    );
                },
                function _deleteBatch(ctx, cb) {
                    var deleteBegin = process.hrtime();

                    // We have to delete the garbage batch if there were any
                    // records in the original batch even if those didn't result
                    // in any instructions. This can be true when all of garbage
                    // consists of 0-byte objects which have no sharks in their
                    // `sharks: []` array.
                    if (readRecords === 0) {
                        self.log.debug(
                            {
                                elapsed: elapsedSince(deleteBegin),
                                reqId: reqId
                            },
                            'No garbage records received, skipping delete.'
                        );
                        cb();
                        return;
                    }

                    self.deleteProcessedGarbage(
                        {
                            batchId: ctx.results.batchId,
                            reqId: reqId
                        },
                        function _onDelete(err, results) {
                            if (err) {
                                self.addCounter(
                                    'mdapiBatchDeleteErrorCount',
                                    1
                                );
                            }
                            self.addCounter('mdapiBatchDeleteCount', 1);
                            self.addCounter(
                                'mdapiBatchDeleteSeconds',
                                elapsedSince(deleteBegin)
                            );

                            //
                            // NOTE: We'd prefer to get the record delete count
                            // from mdapi's response, but that is not currently
                            // available.
                            //
                            if (!err) {
                                self.addCounter(
                                    'mdapiRecordDeleteCount',
                                    readRecords
                                );
                                deletedRecords = readRecords;
                            }

                            self.log[err ? 'error' : 'debug'](
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
            var elapsed = elapsedSince(beginning);

            if (err) {
                self.addCounter('runErrorCountTotal', 1);
            }
            self.addCounter('runSecondsTotal', elapsed);

            self.log[err ? 'error' : 'info'](
                {
                    deletes: deletedRecords,
                    elapsed: elapsed,
                    err: err,
                    reads: readRecords,
                    reqId: reqId,
                    writes: writtenRecords
                },
                'Run complete. Will run again in %d seconds.',
                Math.floor(self.readBatchDelay / 1000)
            );

            if (callback) {
                callback(err);
            }

            // Schedule next run.
            self.nextRunTimer = setTimeout(function _nextRun() {
                self.run();
            }, self.readBatchDelay);

            // For tests
            if (self._runHook) {
                self._runHook({
                    deletes: deletedRecords,
                    elapsed: elapsed,
                    err: err,
                    reads: readRecords,
                    writes: writtenRecords
                });
            }
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
    var recordCount = 0;
    var records = {};
    var timeToFirstRecord;

    //
    // XXX eventually we should really support a garbage batch size parameter
    //     but that requires support in buckets-mdapi.
    //
    self.client.getGCBatch(opts.reqId, function _onBatch(err, garbageBatch) {
        var copies;
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

        assert.ok(
            garbageBatch.garbage.length <= MAX_GARBAGE_RECORDS,
            'should not have too many garbage records, had: ' +
                garbageBatch.garbage.length +
                ' limit: ' +
                MAX_GARBAGE_RECORDS
        );

        //
        // We log the data received at `info` level so that we can correlate
        // the data here with the data at garbage-deleter on the storage zones.
        // If this ends up being too much data, we can increase the processing
        // overhead and trim down the objects but ideally we'd have at minimum:
        //
        //  * batch_id
        //  * bucket_id
        //  * content_length
        //  * id
        //  * owner
        //  * sharks
        //
        // Other info is less useful.
        //
        self.log.info(
            {
                garbageBatch: garbageBatch,
                reqId: opts.reqId
            },
            'Received garbageBatch from buckets-mdapi.'
        );

        if (!garbageBatch.garbage) {
            self.log.error(
                {
                    garbageBatch: garbageBatch
                },
                'Bad garbage batch.'
            );

            // caller will increment error metric.
            callback(new Error('Bad garbage batch'));
            return;
        }

        // NOTE: Ideally we'd track the number of items remaining in the mdapi
        //       as well, but currently that information is not exposed.
        //       MANTA-4892 exists to add this.

        if (timeToFirstRecord === undefined) {
            timeToFirstRecord = elapsedSince(beginning);
        }

        recordCount = garbageBatch.garbage.length;
        for (idx = 0; idx < garbageBatch.garbage.length; idx++) {
            garbageRecord = garbageBatch.garbage[idx];

            // Minimal validation on garbageRecord. We generally assume these
            // are fine since they're generated by buckets-mdapi internally,
            // so we abort the whole run if not since that indicates something
            // there is broken and needs investigation.
            try {
                assert.object(garbageRecord, 'garbageRecord');
                assert.number(
                    garbageRecord.content_length,
                    'garbageRecord.content_length'
                );
                assert.arrayOfObject(
                    garbageRecord.sharks,
                    'garbageRecord.sharks'
                );
                assert.uuid(garbageRecord.id, 'garbageRecord.id (objectId)');
                assert.uuid(garbageRecord.owner, 'garbageRecord.owner');
                assert.string(garbageRecord.name, 'garbageRecord.name');
                for (
                    sharkIdx = 0;
                    sharkIdx < garbageRecord.sharks.length;
                    sharkIdx++
                ) {
                    assert.string(
                        garbageRecord.sharks[sharkIdx].manta_storage_id,
                        'garbageRecord.sharks[' +
                            sharkIdx +
                            '].manta_storage_id'
                    );
                }
            } catch (e) {
                callback(
                    new VError(
                        {
                            cause: e,
                            info: {
                                reqId: opts.reqId,
                                shard: self.shard
                            },
                            record: garbageRecord,
                            name: 'InvalidGarbageRecord'
                        },
                        'Invalid garbage record from ' + self.shard
                    )
                );
                return;
            }

            self.addCounter('mdapiBytesRead', garbageRecord.content_length);

            // NOTE: 0-byte objects will have no "sharks", these will also get
            // pruned out here (since the for loop won't include them in records)
            // but we still want to count them for metrics purposes.
            copies = garbageRecord.sharks.length;
            if (garbageRecord.sharks.length === undefined) {
                copies = 0;
            }
            self.addCounter('mdapiRecordsRead', 1, {copies: copies});

            for (
                sharkIdx = 0;
                sharkIdx < garbageRecord.sharks.length;
                sharkIdx++
            ) {
                storageId = garbageRecord.sharks[sharkIdx].manta_storage_id;

                if (!records.hasOwnProperty(storageId)) {
                    records[storageId] = [];
                }

                // NOTES:
                //
                //  Unlike dir-style Manta, buckets does not track owner and
                //  creator separately. So we only ever have owner here.
                //
                //  The crypto call here is expensive. We might eventually be
                //  limited on CPU.
                //
                records[storageId].push({
                    bucketId: garbageRecord.bucket_id,
                    bytes: garbageRecord.content_length,
                    hash: crypto
                        .createHash('md5')
                        .update(garbageRecord.name)
                        .digest('hex'),
                    objectId: garbageRecord.id,
                    ownerId: garbageRecord.owner,
                    path: garbageRecord.name,
                    shard: self.shard,
                    storageId: storageId
                });

                self.addCounter(
                    'mdapiBytesReadWithCopies',
                    garbageRecord.content_length
                );
                self.addCounter('mdapiRecordsReadWithCopies', 1);
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
            recordCount: recordCount,
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

    self.log.debug({opts: opts}, 'Deleting processed garbage.');

    self.client.deleteGCBatch(opts.batchId, opts.reqId, function _onDeleteBatch(
        err,
        deleteBatchResult
    ) {
        if (err) {
            // NOTE: With Moray we check here whether it's overloaded.
            //       buckets-mdapi doesn't support that.
            self.log.error(
                {
                    batchId: opts.batchId,
                    elapsed: elapsedSince(beginning),
                    err: err,
                    reqId: opts.reqId
                },
                'Error deleting garbage batch.'
            );

            // Caller will update metric for this error.
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
            'Called deleteGCBatch.'
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
    });
};

module.exports = GarbageBucketsConsumer;
