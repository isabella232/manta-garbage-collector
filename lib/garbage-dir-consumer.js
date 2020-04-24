/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

//
// This contains the code for GarbageDirConsumer objects which are used by
// the garbage-dir-consumer program to pull garbage records from moray and
// create instructions files to send to storage servers.
//
// Future:
//
//  - If we read a full batch (dir_batch_size results) we probably should
//    decrease the delay since there are likely to be more records waiting.
//  - Handle overloaded moray by backing off
//  - Should we include shard label on metrics? Cardinality...
//

var assert = require('assert-plus');
var moray = require('moray');
var vasync = require('vasync');

var common = require('../lib/common');

var elapsedSince = common.elapsedSince;
var writeGarbageInstructions = common.writeGarbageInstructions;

function GarbageDirConsumer(opts) {
    var self = this;

    assert.object(opts, 'opts');
    assert.object(opts.config, 'opts.config');
    assert.object(opts.log, 'opts.log');
    assert.optionalString(opts.instructionRoot, 'opts.instructionRoot');
    assert.object(opts.morayConfig, 'opts.morayConfig');
    assert.optionalString(opts.metricPrefix, 'opts.metricPrefix');
    assert.optionalObject(opts.metricsManager, 'opts.metricsManager');
    assert.optionalObject(opts._morayClient, 'opts._morayClient');
    assert.optionalFunc(opts._runHook, 'opts._runHook');
    assert.string(opts.shard, 'opts.shard');

    var metricPrefix = opts.metricPrefix || '';

    // Options for testing
    self._runHook = opts._runHook;
    self.client = opts._morayClient; // allow override for testing
    self.instructionRoot = opts.instructionRoot;

    // Regular options
    self.config = opts.config;
    self.log = opts.log;
    self.metricsManager = opts.metricsManager;
    self.morayConfig = opts.morayConfig;
    self.nextRunTimer = null;
    self.shard = opts.shard;

    if (!self.config.options) {
        self.config.options = {};
    }

    //
    // dir_batch_interval_ms sets the number of *milliseconds* we wait after
    // each run completes before we start the next one. Each run will do a
    // findobjects and then create instructions files from the results. The
    // default value of 1 minute was chosen without any real data and might need
    // to be adjusted based on real-world usage. This parameter can be set via
    // the SAPI configuration.
    //
    // record_read_batch_size sets the number of records to read on each
    // findObjects call (limit option in the moray request). The default value
    // was chosen based on the existing tunings in production systems for the
    // old Accelerated GC system. These might need to be adjusted once we have
    // some production data for this system.
    //
    self.readBatchDelay = self.config.options.dir_batch_interval_ms || 60000;
    self.readBatchSize = self.config.options.dir_batch_size || 200;

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
                help: 'Total number of instructions written.'
            }),
            morayBytesRead: self.metricsManager.collector.counter({
                name: metricPrefix + 'moray_record_bytes_count_total',
                help:
                    'Number of *object* bytes represented by garbage records ' +
                    'read by the dir-consumer from moray.'
            }),
            morayBytesReadWithCopies: self.metricsManager.collector.counter({
                name: metricPrefix + 'moray_record_copies_bytes_count_total',
                help:
                    'Number of *object* bytes represented by garbage records ' +
                    'read by the dir-consumer from moray (including copies).'
            }),
            morayRecordsRead: self.metricsManager.collector.counter({
                name: metricPrefix + 'moray_record_read_count_total',
                help:
                    'Number of moray garbage records read by the dir-consumer.'
            }),
            morayRecordsReadWithCopies: self.metricsManager.collector.counter({
                name: metricPrefix + 'moray_record_copies_read_count_total',
                help:
                    'Number of moray garbage records read by the dir-consumer' +
                    ' (including copies).'
            }),
            runCountTotal: self.metricsManager.collector.counter({
                name: metricPrefix + 'run_count_total',
                help:
                    'Counter incremented every time the dir-consumer looks ' +
                    'for new garbage in moray.'
            }),
            runErrorCountTotal: self.metricsManager.collector.counter({
                name: metricPrefix + 'run_error_count_total',
                help:
                    'Counter incremented every time the dir-consumer looks ' +
                    'for new garbage in moray and has an error.'
            }),
            runSecondsTotal: self.metricsManager.collector.counter({
                name: metricPrefix + 'run_seconds_total',
                help:
                    'Total number of seconds spent scanning moray and writing' +
                    ' instruction files.'
            }),
            morayBatchReadCount: self.metricsManager.collector.counter({
                name: metricPrefix + 'moray_batch_read_count_total',
                help: 'Total number of "batches" of records read from Moray.'
            }),
            morayBatchReadErrorCount: self.metricsManager.collector.counter({
                name: metricPrefix + 'moray_batch_read_error_count_total',
                help:
                    'Total number of errors encountered  reading "batches" ' +
                    'of records read from Moray.'
            }),
            morayBatchReadSeconds: self.metricsManager.collector.counter({
                name: metricPrefix + 'moray_batch_read_seconds_total',
                help:
                    'Total number of seconds spent reading batches of records' +
                    ' from Moray.'
            }),
            morayBatchDeleteCount: self.metricsManager.collector.counter({
                name: metricPrefix + 'moray_batch_delete_count_total',
                help:
                    'Number of deleteMany calls to delete batches of records ' +
                    'from Moray.'
            }),
            morayBatchDeleteErrorCount: self.metricsManager.collector.counter({
                name: metricPrefix + 'moray_batch_delete_error_count_total',
                help:
                    'Number of errors encountered deleting batches of records' +
                    ' from Moray.'
            }),
            morayBatchDeleteSeconds: self.metricsManager.collector.counter({
                name: metricPrefix + 'moray_batch_delete_seconds_total',
                help:
                    'Total number of seconds spent deleting batches of records' +
                    ' from Moray.'
            }),
            morayRecordDeleteCount: self.metricsManager.collector.counter({
                name: metricPrefix + 'moray_record_delete_count_total',
                help:
                    'Total number of processed garbage records deleted from ' +
                    'Moray.'
            })
        };
    } else {
        self.metrics = {};
    }

    self.addCounter('morayBatchReadCount', 0);
    self.addCounter('morayBatchReadErrorCount', 0);
    self.addCounter('morayBatchReadSeconds', 0);

    self.addCounter('instrFileWriteCount', 0);
    self.addCounter('instrFileWriteErrorCount', 0);
    self.addCounter('instrFileWriteSeconds', 0);
    self.addCounter('instrWriteCount', 0);

    self.addCounter('morayBytesRead', 0);
    self.addCounter('morayBytesReadWithCopies', 0);
    // Note: we don't initialize morayRecordsRead here because it's always used
    // with a label copies=<# copies>.
    self.addCounter('morayRecordsReadWithCopies', 0);

    self.addCounter('morayBatchDeleteCount', 0);
    self.addCounter('morayBatchDeleteErrorCount', 0);
    self.addCounter('morayBatchDeleteSeconds', 0);
    self.addCounter('morayRecordDeleteCount', 0);

    self.addCounter('runCountTotal', 0);
    self.addCounter('runErrorCountTotal', 0);
    self.addCounter('runSecondsTotal', 0);
}

GarbageDirConsumer.prototype.addCounter = common.addCounter;
GarbageDirConsumer.prototype.getCounter = common.getCounter;
GarbageDirConsumer.prototype.getGauge = common.getGauge;
GarbageDirConsumer.prototype.setGauge = common.setGauge;

GarbageDirConsumer.prototype.start = function start(callback) {
    var self = this;

    var beginning = process.hrtime();

    self.log.info('Starting GarbageDirConsumer.');

    if (!self.client) {
        self.client = moray.createClient(self.morayConfig);
    }

    //
    // https://github.com/joyent/node-moray/blob/master/docs/man/man3/moray.md
    //
    // says servers should *NOT* handle client.on('error'). So we don't.
    //

    //
    // Unless we use the 'failFast' to the moray client, there is no error
    // emitted. Only 'connect' once it's actually connected.
    //
    self.client.once('connect', function() {
        var delay;

        self.log.info(
            {
                elapsed: elapsedSince(beginning)
            },
            'Connected to Moray.'
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

GarbageDirConsumer.prototype.stop = function stop(callback) {
    var self = this;

    self.log.info('Stopping GarbageDirConsumer.');

    if (self.nextRunTimer !== null) {
        clearTimeout(self.nextRunTimer);
        self.nextRunTimer = null;
    }

    if (callback) {
        callback();
    }
};

GarbageDirConsumer.prototype.run = function run(callback) {
    var self = this;

    var beginning = process.hrtime();
    var deletedRecords = 0;
    var readRecords = 0;
    var writtenRecords = 0;

    self.addCounter('runCountTotal', 1);
    self.log.debug('Running directory consumer.');

    vasync.pipeline(
        {
            arg: {},
            funcs: [
                function _readBatch(ctx, cb) {
                    var readBegin = process.hrtime();

                    self.readGarbageBatch(function _onRead(err, results) {
                        var logLevel = 'debug';

                        self.addCounter('morayBatchReadCount', 1);
                        self.addCounter(
                            'morayBatchReadSeconds',
                            elapsedSince(readBegin)
                        );

                        if (err) {
                            self.addCounter('morayBatchReadErrorCount', 1);
                        } else {
                            readRecords = results.ids.length;

                            //
                            // We log the data received at `info` level if there
                            // is any, so that we can correlate the data here
                            // with the data at garbage-deleter on the storage
                            // zones.  If this ends up being too much data, we
                            // can increase the processing overhead and trim
                            // down the objects but ideally we'd have at
                            // minimum:
                            //
                            //  * _id
                            //  * bytes
                            //  * objectId
                            //  * ownerId
                            //  * shard
                            //  * storageId
                            //
                            // Other info (e.g. path) is less necessary.
                            //
                            if (
                                results.ids.length > 0 ||
                                Object.keys(results.records).length > 0
                            ) {
                                logLevel = 'info';
                            }
                            self.log[logLevel](
                                {
                                    elapsed: elapsedSince(readBegin),
                                    results: results
                                },
                                'Received garbage batch from Moray.'
                            );
                        }

                        ctx.results = results;
                        cb(err);
                    });
                },
                function _writeBatch(ctx, cb) {
                    var writeBegin = process.hrtime();

                    if (Object.keys(ctx.results.records).length === 0) {
                        self.log.debug(
                            {
                                elapsed: elapsedSince(writeBegin)
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
                            records: ctx.results.records
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
                                    results: results
                                },
                                'Finished writing garbage instructions.'
                            );

                            cb(err);
                        }
                    );
                },
                function _deleteBatch(ctx, cb) {
                    var deleteBegin = process.hrtime();

                    self.deleteProcessedGarbage(
                        {
                            ids: ctx.results.ids
                        },
                        function _onDelete(err, results) {
                            if (err) {
                                self.addCounter(
                                    'morayBatchDeleteErrorCount',
                                    1
                                );
                            }

                            self.addCounter('morayBatchDeleteCount', 1);
                            self.addCounter(
                                'morayBatchDeleteSeconds',
                                elapsedSince(deleteBegin)
                            );

                            //
                            // NOTE: We'd prefer to get the record delete count
                            // from Moray's response, but that is not currently
                            // known to be available.
                            //
                            if (!err) {
                                self.addCounter(
                                    'morayRecordDeleteCount',
                                    ctx.results.ids.length
                                );
                                deletedRecords = ctx.results.ids.length;
                            }

                            self.log[err ? 'error' : 'debug'](
                                {
                                    elapsed: elapsedSince(deleteBegin),
                                    err: err,
                                    results: results
                                },
                                'Finished deleting garbage from Moray.'
                            );

                            cb(err);
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

GarbageDirConsumer.prototype.readGarbageBatch = function readGarbageBatch(
    callback
) {
    var self = this;

    assert.func(callback, 'callback');

    var beginning = process.hrtime();
    var findOpts = {};
    var ids = [];
    var records = {};
    var req;
    var timeToFirstRecord;

    findOpts.limit = self.readBatchSize;
    findOpts.sort = {
        attribute: '_id',
        order: 'ASC'
    };

    //
    // When there's an error reading records, we'll fail the whole operation and
    // should end up reading the same records again. This means if a bad record
    // is breaking us, we'll not make progress. But we'll have logged errors and
    // error metrics will be counting up so hopefully these cases can be tracked
    // down and fixed before they become problems.
    //
    // If this becomes really common, we'll probably want to add a mechanism to
    // ignore known-bad records but continue to log and add metrics for the
    // failures.
    //

    req = self.client.findObjects(
        common.FASTDELETE_BUCKET,
        '(_id>=0)',
        findOpts
    );

    req.once('error', function _findError(err) {
        self.log.error(
            {
                elapsed: elapsedSince(beginning),
                err: err,
                timeToFirstRecord: timeToFirstRecord
            },
            'Error reading garbage batch.'
        );

        if (common.isMorayOverloaded(err)) {
            // TODO: automatically back off
            self.log.warn('Warning Moray is overloaded, we should back off.');
        }

        callback(err);
    });

    req.on('record', function _findRecord(obj) {
        var copies;
        var idx;
        var storageId;
        var value = obj.value;

        if (timeToFirstRecord === undefined) {
            timeToFirstRecord = elapsedSince(beginning);
        }

        // XXX verify obj is a good obj
        //     assert value.type === 'object'?
        //     assert value

        self.addCounter('morayBytesRead', value.contentLength);

        // NOTE: 0-byte objects will have no "sharks", these will also get
        // pruned out here (since the for loop won't include them in records)
        // but we still want to count them for metrics purposes.
        copies = value.sharks.length;
        if (value.sharks.length === undefined) {
            copies = 0;
        }
        self.addCounter('morayRecordsRead', 1, {copies: copies});

        for (idx = 0; idx < value.sharks.length; idx++) {
            // XXX explain and make sure the creator || owner stuff is correctly
            // matching what we did before.

            storageId = value.sharks[idx].manta_storage_id;

            if (!records.hasOwnProperty(storageId)) {
                records[storageId] = [];
            }

            records[storageId].push({
                _id: obj._id,
                bytes: value.contentLength,
                objectId: value.objectId,
                ownerId: value.creator || value.owner,
                path: value.key,
                shard: self.shard,
                storageId: storageId
            });

            self.addCounter('morayBytesReadWithCopies', value.contentLength);
            self.addCounter('morayRecordsReadWithCopies', 1);
        }

        // Even if the object had no "sharks", we still add it to the ids list
        // here so that the garbage entry gets deleted from metadata.
        ids.push(obj._id);
    });

    req.once('end', function _findEnd() {
        self.log.debug(
            {
                elapsed: elapsedSince(beginning),
                timeToFirstRecord: timeToFirstRecord
            },
            'Done reading garbage batch.'
        );

        callback(null, {
            ids: ids,
            records: records
        });
    });
};

GarbageDirConsumer.prototype.deleteProcessedGarbage = function deleteProcessedGarbage(
    opts,
    callback
) {
    var self = this;

    var beginning = process.hrtime();
    // Thanks LDAP!
    var filter = '';
    var idx;

    assert.object(opts, 'opts');
    assert.array(opts.ids, 'opts.ids');
    assert.func(callback, 'callback');

    if (opts.ids.length === 0) {
        // Nothing to delete.
        callback();
        return;
    }

    for (idx = 0; idx < opts.ids.length; idx++) {
        filter += '(_id=' + opts.ids[idx] + ')';
    }

    // If we have more than 1 entry, we need an 'or' filter.
    if (opts.ids.length > 1) {
        filter = '(|' + filter + ')';
    }

    self.log.debug({ids: opts.ids, filter: filter}, 'Deleting processed _ids.');

    self.client.deleteMany(common.FASTDELETE_BUCKET, filter, function _onDelete(
        err
    ) {
        self.log.debug(
            {
                elapsed: elapsedSince(beginning),
                err: err
            },
            'Did deleteMany.'
        );

        callback(err);
    });
};

module.exports = GarbageDirConsumer;
