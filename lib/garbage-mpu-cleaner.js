/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

//
// This contains the code for GarbageMpuCleaner objects which are used by
// the garbage-mpu-cleaner program to:
//
//   * pull MPU finalize records from Moray
//   * list and delete the parts and directory using metadataDeleter (which talks to Electric Moray)
//   * delete the manta_upload record
//
// Unlike the other components here, it does not deal with instructions files.
// Deleting the parts will trigger a manta_fastdelete_queue entry which will
// cause the files on disk to be collected using the normal mechanism.
//
// Configuration:
//
//  * mpu_cleanup_age_seconds
//
//    - when we look at manta_uploads, we'll cleanup entries created more than
//      this many seconds ago.
//
//  * mpu_cleanup_batch_size
//
//    - how many manta_upload finalize records to load at once.
//
//  * mpu_cleanup_batch_interval_ms
//
//    - how many milliseconds to wait between findObjects calls on the
//      manta_uploads table.
//
// Future:
//
//  - Should we include shard label on metrics? Cardinality...
//

var assert = require('assert-plus');
var moray = require('moray');
var vasync = require('vasync');

var common = require('../lib/common');

var elapsedSince = common.elapsedSince;

function GarbageMpuCleaner(opts) {
    var self = this;

    assert.object(opts, 'opts');
    assert.object(opts.config, 'opts.config');
    assert.object(opts.log, 'opts.log');
    assert.func(opts.metadataDeleter, 'opts.metadataDeleter');
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

    // Regular options
    self.config = opts.config;
    self.log = opts.log;
    self.metadataDeleter = opts.metadataDeleter;
    self.metricsManager = opts.metricsManager;
    self.morayConfig = opts.morayConfig;
    self.nextRunTimer = null;
    self.shard = opts.shard;

    if (!self.config.options) {
        self.config.options = {};
    }

    //
    // mpu_cleanup_age_seconds is the number of *seconds* old the manta_upload
    // record has to be (according to _mtime) before we'll consider it for GC.
    // The reason for this delay is that once the GC has completed, the upload
    // will be unavailble from the customer's list of uploads and they will not
    // be able to do a get on the upload object. So we want them to have some
    // amount of time to do that before we clean it up. We don't however want to
    // have it *too* far from the delete because doing an action today and
    // having that generate a lot of load days later is unfortunate. The default
    // value here was not picked based on any data so might need to be adjusted
    // once we have production data.
    //
    // mpu_batch_interval_ms sets the number of *milliseconds* we wait after
    // each run completes before we start the next one. Each run will do a
    // findObjects on the manta_uploads bucket and then attempt to delete all
    // related parts and directories before deleting the manta_uploads record.
    // The default value here was chosen without data, it might be adjusted once
    // we have data.
    //
    // mpu_batch_size sets the number of manta_upload records to read on each
    // findObjects call (limit option in the moray request). The default value
    // was chosen basically at random here since we have no real data or targets
    // for the usage of this system. These might need to be adjusted once we
    // have some production data for this system.
    //
    self.mpuCleanupAgeSecs = self.config.options.mpu_cleanup_age_seconds || 300;
    self.readBatchDelay = self.config.options.mpu_batch_interval_ms || 60000;
    self.readBatchSize = self.config.options.mpu_batch_size || 200;

    if (self.metricsManager) {
        self.metrics = {
            runCountTotal: self.metricsManager.collector.counter({
                name: metricPrefix + 'run_count_total',
                help:
                    'Counter incremented every time the mpu-cleaner looks ' +
                    'for new garbage in Moray.'
            }),
            runErrorCountTotal: self.metricsManager.collector.counter({
                name: metricPrefix + 'run_error_count_total',
                help:
                    'Counter incremented every time the mpu-cleaner looks ' +
                    'for new garbage in Moray and has an error.'
            }),
            runSecondsTotal: self.metricsManager.collector.counter({
                name: metricPrefix + 'run_seconds_total',
                help:
                    'Total number of seconds spent scanning Moray and doing ' +
                    'deletes.'
            }),

            morayBatchReadCount: self.metricsManager.collector.counter({
                name: metricPrefix + 'moray_batch_read_count_total',
                help:
                    'Total number of "batches" of manta_upload records ' +
                    'from Moray.'
            }),
            morayBatchReadErrorCount: self.metricsManager.collector.counter({
                name: metricPrefix + 'moray_batch_read_error_count_total',
                help:
                    'Total number of errors encountered reading "batches" ' +
                    'of records from Moray.'
            }),
            morayBatchReadRecordCount: self.metricsManager.collector.counter({
                name: metricPrefix + 'moray_batch_read_record_count_total',
                help:
                    'Total number of "finalize" records read from the ' +
                    'manta_uploads bucket of Moray.'
            }),
            morayBatchReadSeconds: self.metricsManager.collector.counter({
                name: metricPrefix + 'moray_batch_read_seconds_total',
                help:
                    'Total number of seconds spent reading batches of records' +
                    ' from Moray.'
            }),

            morayFinalizeDeleteCount: self.metricsManager.collector.counter({
                name: metricPrefix + 'moray_finalize_delete_count_total',
                help:
                    'Total number of "finalize" manta_upload records ' +
                    'deleted from Moray.'
            }),
            morayFinalizeDeleteErrorCount: self.metricsManager.collector.counter(
                {
                    name:
                        metricPrefix +
                        'moray_finalize_delete_error_count_total',
                    help:
                        'Total number of "finalize" manta_upload records ' +
                        'that failed to be deleted from Moray due to an error.'
                }
            ),
            morayFinalizeDeleteSeconds: self.metricsManager.collector.counter({
                name: metricPrefix + 'moray_finalize_delete_seconds_total',
                help:
                    'Total amount of time in seconds spent deleting ' +
                    '"finalize" manta_upload records from Moray.'
            }),
            morayFinalizeRecordsRemaining: self.metricsManager.collector.gauge({
                name: metricPrefix + 'moray_finalize_records_remaining',
                help:
                    'Number of manta_upload "finalize" records that remain ' +
                    'in Moray after the current collection.'
            })
        };
    } else {
        self.metrics = {};
    }

    self.addCounter('morayBatchReadCount', 0);
    self.addCounter('morayBatchReadErrorCount', 0);
    self.addCounter('morayBatchReadRecordCount', 0);
    self.addCounter('morayBatchReadSeconds', 0);

    self.addCounter('morayFinalizeDeleteCount', 0);
    self.addCounter('morayFinalizeDeleteErrorCount', 0);
    self.addCounter('morayFinalizeDeleteSeconds', 0);
    self.setGauge('morayFinalizeRecordsRemaining', 0);

    self.addCounter('runCountTotal', 0);
    self.addCounter('runErrorCountTotal', 0);
    self.addCounter('runSecondsTotal', 0);
}

GarbageMpuCleaner.prototype.addCounter = common.addCounter;
GarbageMpuCleaner.prototype.getCounter = common.getCounter;
GarbageMpuCleaner.prototype.getGauge = common.getGauge;
GarbageMpuCleaner.prototype.setGauge = common.setGauge;

GarbageMpuCleaner.prototype.start = function start(callback) {
    var self = this;

    var beginning = process.hrtime();

    self.log.info('Starting GarbageMpuCleaner');

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
            'Connected to Moray'
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

GarbageMpuCleaner.prototype.stop = function stop(callback) {
    var self = this;

    self.log.info('Stopping GarbageMpuCleaner');

    if (self.nextRunTimer !== null) {
        clearTimeout(self.nextRunTimer);
        self.nextRunTimer = null;
    }

    if (callback) {
        callback();
    }
};

GarbageMpuCleaner.prototype.run = function run(callback) {
    var self = this;

    var beginning = process.hrtime();
    var deletedParts = 0;
    var deletedPartsDirs = 0;
    var deletedRecords = 0;
    var readRecords = 0;
    var remainingRecords = -1;

    self.addCounter('runCountTotal', 1);
    self.log.debug('Running directory consumer.');

    vasync.pipeline(
        {
            arg: {},
            funcs: [
                function _readBatch(ctx, cb) {
                    var readBegin = process.hrtime();

                    self.readFinalizeRecords(function _onRead(err, results) {
                        var logLevel = 'debug';
                        var records;

                        self.addCounter('morayBatchReadCount', 1);
                        self.addCounter(
                            'morayBatchReadSeconds',
                            elapsedSince(readBegin)
                        );

                        if (err) {
                            self.addCounter('morayBatchReadErrorCount', 1);
                        } else {
                            records = results.records;
                            remainingRecords = results.remaining;

                            self.setGauge(
                                'morayFinalizeRecordsRemaining',
                                remainingRecords
                            );
                            readRecords = records.length;

                            //
                            // We log the data received at `info` level if there
                            // is any, so that we have an audit record of
                            // everything we attempted to delete. If this ends
                            // up being too much data, we can increase the
                            // processing overhead and trim down the objects but
                            // ideally we'd have at minimum:
                            //
                            //  * _id
                            //  * _mtime
                            //  * value.uploadId
                            //  * value.objectId
                            //  * value.ownerId
                            //
                            if (readRecords > 0) {
                                self.addCounter(
                                    'morayBatchReadRecordCount',
                                    readRecords
                                );
                                logLevel = 'info';
                            }
                            self.log[logLevel](
                                {
                                    elapsed: elapsedSince(readBegin),
                                    records: records
                                },
                                'Received MPU garbage batch from moray.'
                            );
                            ctx.records = records;
                        }

                        cb(err);
                    });
                },
                function _deleteMetadata(ctx, cb) {
                    ctx.successfullyDeleted = [];

                    if (ctx.records.length === 0) {
                        self.log.debug(
                            'No records received, no metadata objects to ' +
                                'delete.'
                        );

                        cb();
                        return;
                    }

                    // GarbageMpuPartsCleaner logs its own metrics for the
                    // metadata deletes.
                    self.metadataDeleter(ctx.records, function _onDeleted(
                        err,
                        results
                    ) {
                        var idx;

                        self.log.debug(
                            {
                                results: results
                            },
                            'self.metadataDeleter results'
                        );

                        if (!err) {
                            //
                            // We passed the metadataDeleter the whole batch of
                            // records to cleanup and it tried to do so through
                            // electric-moray. For any records it successfully
                            // cleaned up, we'll delete the manta_upload record
                            // and never see it again. If it wasn't successful,
                            // we'll try again on the next run.
                            //
                            assert.array(results.successes);
                            ctx.successfullyDeleted = results.successes;

                            // We only get a "successes" entry when the parts
                            // directory was deleted.
                            deletedPartsDirs += results.successes.length;

                            for (
                                idx = 0;
                                idx < results.successes.length;
                                idx++
                            ) {
                                deletedParts +=
                                    results.successes[idx].deletedParts || 0;
                            }
                        }

                        cb(err);
                    });
                },
                function _deleteMantaUploadRecords(ctx, cb) {
                    var deleteBegin = process.hrtime();

                    if (ctx.records.length === 0) {
                        self.log.debug(
                            'No records received, no manta_upload records to ' +
                                'delete.'
                        );

                        cb();
                        return;
                    }

                    if (ctx.successfullyDeleted.length === 0) {
                        self.log.info(
                            {
                                records: ctx.records
                            },
                            'Did not delete any manta records, not removing ' +
                                'manta_upload record.'
                        );
                        cb();
                        return;
                    }

                    vasync.forEachPipeline(
                        {
                            func: function(rec, _cb) {
                                self.log.debug(
                                    {rec: rec},
                                    'Will delete manta_upload record'
                                );

                                self.client.delObject(
                                    common.MPU_FINALIZE_BUCKET,
                                    rec.key,
                                    {},
                                    function _delObject(err) {
                                        self.log.debug(
                                            {
                                                err: err
                                            },
                                            'delObject complete.'
                                        );

                                        if (err) {
                                            self.addCounter(
                                                'morayFinalizeDeleteErrorCount',
                                                1
                                            );
                                        } else {
                                            deletedRecords++;
                                            self.addCounter(
                                                'morayFinalizeDeleteCount',
                                                1
                                            );
                                        }

                                        _cb(err);
                                    }
                                );
                            },
                            inputs: ctx.successfullyDeleted
                        },
                        function _deletedMantaUploadRecords(err) {
                            var elapsed = elapsedSince(deleteBegin);

                            self.log.info(
                                {
                                    elapsed: elapsed,
                                    err: err,
                                    successfullyDeleted: ctx.successfullyDeleted
                                },
                                'Deleted manta_upload records.'
                            );

                            self.addCounter(
                                'morayFinalizeDeleteSeconds',
                                elapsed
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
                    deletedParts: deletedParts,
                    deletedPartsDirs: deletedPartsDirs,
                    deletedRecords: deletedRecords,
                    elapsed: elapsed,
                    err: err,
                    reads: readRecords,
                    remainingRecords: remainingRecords
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
                    deletedParts: deletedParts,
                    deletedPartsDirs: deletedPartsDirs,
                    deletedRecords: deletedRecords,
                    deletes: deletedRecords,
                    elapsed: elapsed,
                    err: err,
                    reads: readRecords,
                    remainingRecords: remainingRecords
                });
            }
        }
    );
};

GarbageMpuCleaner.prototype.readFinalizeRecords = function readFinalizeRecords(
    callback
) {
    var self = this;

    assert.func(callback, 'callback');

    var beginning = process.hrtime();
    var findOpts = {};
    var filter;
    var maxCount = 0;
    var now = Date.now();
    var records = [];
    var req;
    var timeToFirstRecord;

    filter = '(_mtime<=' + (now - self.mpuCleanupAgeSecs * 1000) + ')';

    self.log.debug(
        {
            now: now,
            mpuCleanupAgeSecs: self.mpuCleanupAgeSecs,
            filter: filter
        },
        'readFinalizeRecords filter'
    );

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

    req = self.client.findObjects(common.MPU_FINALIZE_BUCKET, filter, findOpts);

    req.once('error', function _findError(err) {
        self.log.error(
            {
                elapsed: elapsedSince(beginning),
                err: err,
                timeToFirstRecord: timeToFirstRecord
            },
            'Error reading MPU garbage batch.'
        );

        if (common.isMorayOverloaded(err)) {
            // TODO: automatically back off
            self.log.warn('Warning Moray is overloaded, we should back off.');
        }

        callback(err);
    });

    req.on('record', function _findRecord(obj) {
        var value = obj.value;

        if (obj._count > maxCount) {
            maxCount = obj._count;
        }

        if (timeToFirstRecord === undefined) {
            timeToFirstRecord = elapsedSince(beginning);
        }

        // We add an _ageSecs here just to make it easier for humans looking at
        // the logs.
        obj._ageSecs = Math.ceil((now - obj._mtime) / 1000);

        self.log.debug(
            {
                finalizingType: value.finalizingType,
                id: obj._id,
                key: obj.key,
                mtime: obj._mtime,
                objectId: value.objectId,
                objectPath: value.objectPath,
                uploadId: value.uploadId
            },
            'Found "' + value.finalizingType + '" finalizing object.'
        );

        records.push(obj);
    });

    req.once('end', function _findEnd() {
        self.log.debug(
            {
                elapsed: elapsedSince(beginning),
                timeToFirstRecord: timeToFirstRecord,
                maxCount: maxCount,
                records: records.length
            },
            'Done reading MPU garbage batch.'
        );

        callback(null, {
            records: records,
            remaining: maxCount - records >= 0 ? maxCount - records : 0
        });
    });
};

module.exports = GarbageMpuCleaner;
