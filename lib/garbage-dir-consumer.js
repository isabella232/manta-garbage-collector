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

var assert = require('assert-plus');
var moray = require('moray');
var vasync = require('vasync');

var common = require('../lib/common');

var elapsedSince = common.elapsedSince;
var writeGarbageInstructions = common.writeGarbageInstructions;

function GarbageDirConsumer(opts) {
    var self = this;

    // XXX assert params

    self.config = opts.config;
    self.deleteQueue = {};
    self.log = opts.log;
    self.metricsManager = opts.metricsManager;
    self.morayConfig = opts.morayConfig;
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

GarbageDirConsumer.prototype.start = function start() {
    var self = this;

    var beginning = process.hrtime();

    self.log.info('Starting');

    self.client = moray.createClient(self.morayConfig);

    // XXX docs seem to say we shouldn't look at .on('error')?

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
            'Startup Complete. Will delay %d seconds before first read.',
            delay
        );

        self.nextRunTimer = setTimeout(function _firstRun() {
            self.run();
        }, delay * 1000);
    });
};

GarbageDirConsumer.prototype.stop = function stop() {
    var self = this;

    self.log.info('Stopping');

    if (self.nextRunTimer !== null) {
        clearTimeout(self.nextRunTimer);
        self.nextRunTimer = null;
    }
};

GarbageDirConsumer.prototype.run = function run() {
    var self = this;

    var beginning = process.hrtime();

    self.log.info('Running Consumer.');

    vasync.pipeline(
        {
            arg: {},
            funcs: [
                function _readBatch(ctx, cb) {
                    var readBegin = process.hrtime();

                    self.readGarbageBatch(function _onRead(err, results) {
                        self.log.info(
                            {
                                elapsed: elapsedSince(readBegin),
                                err: err,
                                results: results
                            },
                            'Finished reading garbage batch.'
                        );
                        ctx.results = results;
                        cb(err);
                    });
                },
                function _writeBatch(ctx, cb) {
                    var writeBegin = process.hrtime();

                    writeGarbageInstructions(
                        self,
                        {
                            records: ctx.results.records
                        },
                        function _onWrite(err, results) {
                            self.log.info(
                                {
                                    elapsed: elapsedSince(writeBegin),
                                    err: err,
                                    results: results
                                },
                                'Finished writing garbage instructions.'
                            );
                            cb();
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
                            self.log.info(
                                {
                                    elapsed: elapsedSince(deleteBegin),
                                    err: err,
                                    results: results
                                },
                                'Finished deleting garbage from Moray.'
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
                    err: err
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

GarbageDirConsumer.prototype.readGarbageBatch = function readGarbageBatch(
    callback
) {
    var self = this;

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

    findOpts.limit = self.readBatchSize;
    findOpts.sort = {
        attribute: '_id',
        order: 'ASC'
    };

    // TODO: assert that records is empty? We shouldn't have any because either
    // we just started, or previous loop should have cleared.
    // What happens on 'error'? Can we have some that were read before we got
    // the error?

    req = self.client.findObjects(
        common.FASTDELETE_BUCKET,
        '(_id>=0)',
        findOpts
    );

    req.once('error', function(err) {
        self.log.error(
            {
                elapsed: elapsedSince(beginning),
                err: err,
                timeToFirstRecord: timeToFirstRecord
            },
            'Error reading garbage batch.'
        );

        if (common.isMorayOverloaded(err)) {
            // XXX
            self.log.info('XXX Moray is overloaded, we should back off.');
        }

        callback(err);
    });

    req.on('record', function(obj) {
        var idx;
        var storageId;
        var value = obj.value;

        if (timeToFirstRecord === undefined) {
            timeToFirstRecord = elapsedSince(beginning);
        }

        // XXX verify obj is a good obj
        //     assert value.type === 'object'?

        counters.totalBytes += value.contentLength;
        counters.totalObjects++;

        // XXX we have a _count? Can we use that?

        // NOTE: 0-byte objects will have no "sharks", these will also get
        // pruned out here (since the for loop won't include them in records)
        // but we still want to count them for metrics purposes.
        //
        // TODO: will we remove them with this?

        if (counters.totalObjectsByCopies[value.sharks.length] === undefined) {
            counters.totalObjectsByCopies[value.sharks.length] = 0;
        }
        counters.totalObjectsByCopies[value.sharks.length]++;

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

            counters.totalBytesWithCopies += value.contentLength;
            counters.totalObjectsWithCopies++;
        }

        // XXX only if we had at least one shark? :thinking_face:
        ids.push(obj._id);
    });

    req.on('end', function() {
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
