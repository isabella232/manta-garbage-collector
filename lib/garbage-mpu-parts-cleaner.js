/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

//
// This contains the code for GarbageMpuPartsCleaner objects which are used by
// the garbage-mpu-cleaner program to:
//
//   * list the parts for each upload using Electric Moray
//   * delete the parts and directory using Electric Moray
//
// Unlike the other components here, MPU cleaner does not deal with instructions
// files.  Deleting the parts will trigger a manta_fastdelete_queue entry which
// will cause the files on disk to be collected using the normal mechanism.
//
// Configuration:
//
//    - None currently.
//
// Future:
//
//  - Should we include shard label on metrics? Cardinality...
//

var path = require('path');

var assert = require('assert-plus');
var jsprim = require('jsprim');
var moray = require('moray');
var uuid = require('uuid/v4');
var vasync = require('vasync');
var verror = require('verror');

var common = require('../lib/common');

var elapsedSince = common.elapsedSince;

var MAX_PARTS = common.MAX_MPU_PARTS;

// These next two functions are copied from muskie:lib/uploads/common.js

var MIN_PREFIX_LEN = 1;
var MAX_PREFIX_LEN = 4;

/*
 * Given an upload ID, extract the prefix length from the last character in the
 * id.
 */
function idToPrefixLen(id) {
    assert.uuid(id, 'id');

    var c = id.charAt(id.length - 1);
    var len = jsprim.parseInteger(c, {base: 16});
    assert.number(len, 'invalid hex value ' + c + 'from uuid ' + id);

    return len;
}

/*
 * Given an upload ID and a prefix length, returns the prefix to use for the
 * parent directory of the upload directory, also referred to as the "prefix"
 * directory.
 *
 * For example, for the input id '0bb83e47-32df-4833-a6fd-94d77e8c7dd3' and a
 * prefix length of 2, this function will return '0b'.
 */
function idToPrefix(id, prefixLen) {
    assert.uuid(id, 'id');
    assert.number(prefixLen, 'prefixLen');
    assert.ok(
        prefixLen >= MIN_PREFIX_LEN && prefixLen <= MAX_PREFIX_LEN,
        'prefix len ' + prefixLen + ' not in range'
    );

    return id.substring(0, prefixLen);
}

function GarbageMpuPartsCleaner(opts) {
    var self = this;

    assert.object(opts, 'opts');
    assert.object(opts.config, 'opts.config');
    assert.object(opts.log, 'opts.log');
    assert.object(opts.emorayConfig, 'opts.emorayConfig');
    assert.optionalString(opts.metricPrefix, 'opts.metricPrefix');
    assert.optionalObject(opts.metricsManager, 'opts.metricsManager');
    assert.optionalObject(opts._emorayClient, 'opts._emorayClient');

    var metricPrefix = opts.metricPrefix || '';

    // Create a serializing queue for deletes since we don't want to overwhelm
    // electric-moray by making more than one request at a time.
    self.queue = vasync.queue(self.metadataDelete.bind(self), 1);

    // This gets set to true after start() has completed and we are connected to
    // electric-moray. It'll get set false again on stop().
    self.running = false;

    // Options for testing
    self.client = opts._emorayClient; // allow override for testing

    // Regular options
    self.config = opts.config;
    self.log = opts.log;
    self.metricsManager = opts.metricsManager;
    self.emorayConfig = opts.emorayConfig;

    if (!self.config.options) {
        self.config.options = {};
    }

    if (self.metricsManager) {
        self.metrics = {
            dirDeleteErrors: self.metricsManager.collector.counter({
                name: metricPrefix + 'dir_delete_error_count_total',
                help:
                    'Number of times we tried to delete an "uploads" ' +
                    'directory and encountered an error.'
            }),
            dirDeleteAttempts: self.metricsManager.collector.counter({
                name: metricPrefix + 'dir_delete_attempts_count_total',
                help:
                    'Number of times we tried to delete an "uploads" ' +
                    'directory.'
            }),
            dirDeleteSuccessCount: self.metricsManager.collector.counter({
                name: metricPrefix + 'dir_delete_success_count_total',
                help:
                    'Number of times we tried to delete an "uploads" ' +
                    'directory and succeeded.'
            }),
            dirDeleteSeconds: self.metricsManager.collector.counter({
                name: metricPrefix + 'dir_delete_seconds_total',
                help:
                    'Total number of seconds spent deleting "uploads" ' +
                    'directories.'
            }),
            partDeleteBatchAttempts: self.metricsManager.collector.counter({
                name: metricPrefix + 'part_delete_batch_attempt_count_total',
                help: 'Number of attempts to delete batches of parts objects.'
            }),
            partDeleteBatchErrors: self.metricsManager.collector.counter({
                name: metricPrefix + 'part_delete_batch_error_count_total',
                help:
                    'Number of times a batch delete of parts objects was ' +
                    'attempted and an error occurred.'
            }),
            partDeleteBatchSeconds: self.metricsManager.collector.counter({
                name: metricPrefix + 'part_delete_batch_seconds_total',
                help:
                    'Total number of seconds spent deleting batches of ' +
                    'parts objects.'
            }),
            partDeleteSuccessCount: self.metricsManager.collector.counter({
                name: metricPrefix + 'part_delete_success_count_total',
                help: 'Number of parts objects successfully deleted.'
            }),

            partListAttempts: self.metricsManager.collector.counter({
                name: metricPrefix + 'part_list_attempts_count_total',
                help:
                    'Number of times listObjects was used to list parts ' +
                    'objects.'
            }),
            partListErrors: self.metricsManager.collector.counter({
                name: metricPrefix + 'part_list_error_count_total',
                help:
                    'Number of times listObjects was used to list parts ' +
                    'objects and an error occurred.'
            }),
            partListRecordCount: self.metricsManager.collector.counter({
                name: metricPrefix + 'part_list_record_count_total',
                help: 'Total number of parts found through findObjects.'
            }),
            partListSeconds: self.metricsManager.collector.counter({
                name: metricPrefix + 'part_list_seconds_total',
                help: 'Total number of seconds spent listing parts objects.'
            })
        };
    } else {
        self.metrics = {};
    }

    self.addCounter('dirDeleteErrors', 0);
    self.addCounter('dirDeleteAttempts', 0);
    self.addCounter('dirDeleteSuccessCount', 0);
    self.addCounter('dirDeleteSeconds', 0);

    self.addCounter('partDeleteBatchAttempts', 0);
    self.addCounter('partDeleteBatchErrors', 0);
    self.addCounter('partDeleteBatchSeconds', 0);
    self.addCounter('partDeleteSuccessCount', 0);

    self.addCounter('partListAttempts', 0);
    self.addCounter('partListErrors', 0);
    self.addCounter('partListRecordCount', 0);
    self.addCounter('partListSeconds', 0);
}

GarbageMpuPartsCleaner.prototype.addCounter = common.addCounter;
GarbageMpuPartsCleaner.prototype.getCounter = common.getCounter;
GarbageMpuPartsCleaner.prototype.getGauge = common.getGauge;
GarbageMpuPartsCleaner.prototype.setGauge = common.setGauge;

GarbageMpuPartsCleaner.prototype.start = function start(callback) {
    var self = this;

    var beginning = process.hrtime();

    self.log.info('Starting GarbageMpuPartsCleaner');

    if (!self.client) {
        self.client = moray.createClient(self.emorayConfig);
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
        self.log.info(
            {
                elapsed: elapsedSince(beginning)
            },
            'Connected to Electric Moray'
        );

        self.running = true;

        callback();
    });
};

//
// It is not intended that you can run .stop() and then .start() again. The
// .stop() here is just for shutting down.
//
GarbageMpuPartsCleaner.prototype.stop = function stop(callback) {
    var self = this;

    self.log.info('Stopping GarbageMpuPartsCleaner');

    self.queue.close();
    self.running = false;

    if (callback) {
        callback();
    }
};

GarbageMpuPartsCleaner.prototype.deleteMantaDir = function deleteMantaDir(
    dirname,
    callback
) {
    var self = this;

    var deleteBegin = process.hrtime();

    self.addCounter('dirDeleteAttempts', 1);

    self.client.delObject(common.MANTA_BUCKET, dirname, {}, function _onDelDir(
        err
    ) {
        var elapsed = elapsedSince(deleteBegin);

        if (err && verror.hasCauseWithName(err, 'ObjectNotFoundError')) {
            self.log.info(
                {
                    bucket: common.MANTA_BUCKET,
                    dir: dirname
                },
                'Directory already gone when we tried to delete.'
            );
            callback();
            return;
        } else if (err) {
            self.addCounter('dirDeleteErrors', 1);

            self.log.error(
                {
                    bucket: common.MANTA_BUCKET,
                    dir: dirname,
                    elapsed: elapsed,
                    err: err
                },
                'Failed to delete parts directory.'
            );
        } else {
            self.addCounter('dirDeleteSuccessCount', 1);

            self.log.info(
                {
                    bucket: common.MANTA_BUCKET,
                    dir: dirname,
                    elapsed: elapsed
                },
                'Deleted parts directory.'
            );
        }

        self.addCounter('dirDeleteSeconds', elapsed);

        callback(err);
    });
};

GarbageMpuPartsCleaner.prototype.deleteMantaParts = function deleteMantaParts(
    opts,
    callback
) {
    var self = this;

    assert.object(opts.record, 'opts.record');
    assert.object(opts.record.value, 'opts.record.value');
    assert.uuid(opts.record.value.owner, 'opts.record.value.owner');
    assert.string(opts.uploadDirname, 'opts.uploadDirname');
    assert.func(callback, 'callback');

    var deleteBatch = [];
    var deletedParts = 0;
    var filter;
    // The actual limit for the Manta v2 MPU is 800 (MAX_PARTS) parts, but we
    // set the limit to MAX_PARTS + 1 here so that we can blow up if there were
    // too many. Because in that case we might not be collecting correctly.
    var findOpts = {limit: MAX_PARTS + 1};
    var listBegin;
    var record = opts.record;
    var req;
    var uploadDirname = opts.uploadDirname;

    //
    // Note: Since we know the uploadDirname was generated here from uuids and
    // '/uploads/', we know it doesn't contain any of the characters that are
    // required to be escaped by LDAP. But we'll assert here just to make extra
    // sure.
    //
    assert.ok(
        // eslint-disable-next-line no-useless-escape
        uploadDirname.match(/^[\/a-z0-9-]+$/),
        'uploadDirname must be LDAP friendly.'
    );

    findOpts.hashkey = uploadDirname;
    filter =
        '(&(dirname=' + uploadDirname + ')(owner=' + record.value.owner + '))';

    self.log.debug(
        {
            filter: filter,
            findOpts: findOpts
        },
        'LDAP filter for findObjects.'
    );

    self.addCounter('partListAttempts', 1);
    listBegin = process.hrtime();

    req = self.client.findObjects(common.MANTA_BUCKET, filter, findOpts);

    req.once('error', function _findError(err) {
        self.log.error(
            {
                record: record,
                elapsed: elapsedSince(listBegin),
                err: err
            },
            'Error reading MPU upload data.'
        );

        self.addCounter('partListErrors', 1);

        if (common.isMorayOverloaded(err)) {
            // TODO: automatically back off
            self.log.warn('Warning Moray is overloaded, we should back off.');
        }

        callback(err);
    });

    req.on('record', function _findRecord(obj) {
        var value = obj.value;

        deleteBatch.push({
            operation: 'delete',
            bucket: common.MANTA_BUCKET,
            key: obj.key,
            options: {
                etag: obj._etag,
                // These "headers" are required in order for the delete to
                // trigger a manta_fastdelete_queue entry.
                headers: {
                    'x-muskie-prev-metadata': value,
                    'x-muskie-snaplinks-disabled': true
                }
            }
        });
    });

    req.once('end', function _findEnd() {
        var deleteBegin = process.hrtime();
        var elapsed = elapsedSince(listBegin);
        var reqId = uuid();

        self.log.debug(
            {
                deleteBatch: deleteBatch,
                elapsed: elapsed
            },
            'Finished finding MPU upload parts.'
        );

        assert.ok(
            deleteBatch.length <= MAX_PARTS,
            '%d parts should be enough for everyone. %d is too many.',
            MAX_PARTS,
            deleteBatch.length
        );

        self.addCounter('partListSeconds', elapsed);

        if (deleteBatch.length === 0) {
            self.log.info(
                {
                    uploadDir: uploadDirname
                },
                'No parts objects found, no need for batch delete.'
            );

            callback(null, {
                deletedParts: deletedParts
            });
            return;
        } else {
            self.addCounter('partListRecordCount', deleteBatch.length);
            self.addCounter('partDeleteBatchAttempts', 1);

            self.client.batch(deleteBatch, {req_id: reqId}, function _didBatch(
                err,
                res
            ) {
                var logLevel = 'debug';

                elapsed = elapsedSince(deleteBegin);

                self.addCounter('partDeleteBatchSeconds', elapsed);

                if (err) {
                    self.addCounter('partDeleteBatchErrors', 1);
                    logLevel = 'error';
                } else {
                    assert.array(res.etags, 'res.etags');
                    self.addCounter('partDeleteSuccessCount', res.etags.length);
                    deletedParts = res.etags.length;

                    //
                    // Moray's docs say that the operations will be performed in
                    // a transaction so that "all or none succeed." As such, we
                    // should never see a number of 'res.etags' results that
                    // doesn't match the number of batch elements we passed in.
                    // If we do, that's a bug in Moray most likely, so we just
                    // abort to save as much state as possible.
                    //
                    assert.equal(
                        res.etags.length,
                        deleteBatch.length,
                        'Moray batch should have handled all operations.'
                    );

                    self.log.info(
                        {
                            dir: uploadDirname,
                            elapsed: elapsed
                        },
                        'Deleted %d/%d parts objects.',
                        res.etags.length,
                        deleteBatch.length
                    );
                }

                self.log[logLevel](
                    {
                        deleteBatchLength: deleteBatch.length,
                        elapsed: elapsed,
                        etags: res ? res.etags : [],
                        err: err,
                        res: res,
                        reqId: reqId
                    },
                    'Batch delete.'
                );

                callback(err, {
                    deletedParts: deletedParts
                });
            });
        }
    });
};

GarbageMpuPartsCleaner.prototype.metadataDeleteOne = function metadataDeleteOne(
    record,
    callback
) {
    var self = this;

    var prefix;
    var uploadDirname;

    // Find prefix using the "magic" last character the same way muskie does.
    prefix = idToPrefix(
        record.value.uploadId,
        idToPrefixLen(record.value.uploadId)
    );
    uploadDirname = path.join(
        '/',
        record.value.owner,
        'uploads',
        prefix,
        record.value.uploadId
    );

    self.deleteMantaParts(
        {
            record: record,
            uploadDirname: uploadDirname
        },
        function _onDeletedParts(err, results) {
            if (err) {
                callback(err);
                return;
            }

            assert.number(results.deletedParts, 'results.deletedParts');

            // Didn't have a problem deleting the parts, so delete the directory.
            self.deleteMantaDir(uploadDirname, function _deletedDir(delErr) {
                if (delErr) {
                    callback(delErr);
                    return;
                }

                // XXX Verify we have record._id and key
                callback(null, {
                    _id: record._id,
                    deletedParts: results.deletedParts,
                    key: record.key
                });
            });
        }
    );
};

GarbageMpuPartsCleaner.prototype.metadataDelete = function metadataDelete(
    data,
    callback
) {
    var self = this;

    self.log.debug(
        {
            data: data
        },
        'Got load of data to delete through electric-moray.'
    );

    vasync.forEachPipeline(
        {
            func: self.metadataDeleteOne.bind(self),
            inputs: data.objs
        },
        function _doneDeleting(err, results) {
            self.log.debug(
                {
                    err: err,
                    results: results
                },
                'Done metadataDelete.'
            );

            callback(err, {
                successes: err ? [] : results.successes
            });
        }
    );
};

//
// This is what the GarbageMpuCleaner objects call to queue up a delete.
//
GarbageMpuPartsCleaner.prototype.metadataDeleter = function metadataDeleter(
    objs,
    callback
) {
    var self = this;

    self.log.debug(
        {
            objs: objs,
            queue: self.queue
        },
        'Pushing onto metadataDelete queue.'
    );

    self.queue.push({objs: objs}, callback);
};

module.exports = GarbageMpuPartsCleaner;
