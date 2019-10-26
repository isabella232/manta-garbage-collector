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
//   manta_fastdelete_queue
//
// Moray bucket(s) for a directory-based Manta installation, and will write them
// out to local "instructions" files in:
//
//  * /var/spool/manta_gc/<storageId>/*
//
// where they will be picked up and transferred to the storage zones for
// processing. Once records are written out locally, they will be removed from
// moray.
//
// The Instructions files have:
//
//  * objectId
//  * creatorId/ownerId
//  * shardId
//  * storageId
//  * bytes (for metrics/auditing)
//
// and therefore contain everything that's necessary to actually collect garbage
// on the individual storage/mako zones ("sharks").
//
// For the "buckets" project, there is not currently any way to collect garbage
// (See https://jira.joyent.us/browse/MANTA-4320) but when there is, it has been
// requested that the garbage be written with separate records per mako from the
// beginning. If that is done, this program will also never need to be used with
// "buckets".
//
// Config Options:
//
//  * moray shard(s) info
//  * frequency of collection
//  * size of batch
//
//
// TODO:
//
//  * startup should ensure delegated dataset
//  * mkdir /var/spool/manta_gc if it doesn't exist (on startup) ... unless we're guaranteed because delegated
//  * Add artedi collector (and node-triton-metrics?)
//  * Test that moray going away and coming back does the right thing
//  * Test when moray down initially then comes up
//  * Test when can't write to spool dir (disk full, etc)
//  * Test when can't delete from moray (Keep writing out same records? Eww.)
//  * Test when we fail writing *some* files, but write others how could this happen?
//  * What's the max size for a moray filter? We shouldn't read more than we can delete.
//
//  IDEA: keep first and last _id from prev read, so we can check that at least
//        window is moving?
//

var fs = require('fs');
var path = require('path');
var util = require('util');

var assert = require('assert-plus');
var moray = require('moray');
var uuid = require('uuid/v4');
var vasync = require('vasync');
var verror = require('verror');
var VError = verror.VError;

// XXX rename this common before I die
var common = require('../lib/common.new');


function GarbageDirConsumer(opts) {
    var self = this;

    // XXX assert params

    var morayConfig;

    self.config = opts.config;
    self.deleteQueue = {};
    self.log = opts.log;
    self.morayConfig = opts.morayConfig;
    self.shard = opts.shard;
    self.stats = {
        deletedByShard: {}, // XXX
        readByShard: {}     // XXX
    };

    //
    self.readBatchDelay = self.config.options.record_read_batch_delay || 13;
    self.readBatchSize = self.config.options.record_read_batch_size || 666;
}

GarbageDirConsumer.prototype.start = function start(callback) {
    var self = this;

    self.log.info('Starting');

    self.client = moray.createClient(self.morayConfig);

    // XXX docs seem to say we shouldn't look at .on('error')?

    self.client.once('connect', function () {
        var delay;

        self.log.info('Connected to Moray');

        // Pick a random offset between 0 and self.readBatchDelay for the first
        // run, in attempt to prevent all shards running at the same time.
        delay = Math.floor(Math.random() * self.readBatchDelay);

        self.log.info('Startup Complete. Will delay %d seconds before first read.', delay);

        setTimeout(function _firstRun() {
            self.run();
        }, delay * 1000);
    });
};

GarbageDirConsumer.prototype.run = function run() {
    var self = this;

    self.log.info('run()');

    // TODO: add timers

    vasync.pipeline({
        arg: {},
        funcs: [
            function _readBatch(ctx, cb) {
                self.readGarbageBatch(function _onRead(err, results) {
                    self.log.info({results: results}, 'readGarbageBatch() results');
                    ctx.results = results;
                    cb(err);
                });
            }, function _writeBatch(ctx, cb) {
                self.writeGarbageInstructions({
                    records: ctx.results.records
                }, function _onWrite(err, results) {
                    self.log.info({results: results}, 'writeGarbageInstructions() results');
                    cb();
                });
            }, function _deleteBatch(ctx, cb) {
                self.deleteProcessedGarbage({
                    ids: ctx.results.ids,
                }, function _onDelete(err, results) {
                    self.log.info({results: results}, 'deleteProcessedGarbage() results');
                    cb();
                });
            }
        ]
    }, function _onPipeline(err) {
        if (err) {
            self.log.error({err: err}, 'Had an error.');
            // XXX do we need to clear the results?
        }

        // Schedule next run.
        self.log.info('Will run again in %d seconds.', self.readBatchDelay);
        setTimeout(function _firstRun() {
            self.run();
        }, self.readBatchDelay * 1000);
    });
};

// TODO: add a timer
GarbageDirConsumer.prototype.readGarbageBatch = function readGarbageBatch(callback) {
    var self = this;

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

    findOpts.limit = self.readBatchSize;
    findOpts.sort = {
        attribute: '_id',
        order: 'ASC'
    };

    // TODO:
    //
    //  add timer to first 'record'
    //  add timer for whole batch "read"


    // TODO: assert that records is empty? We shouldn't have any because either
    // we just started, or previous loop should have cleared.
    // What happens on 'error'? Can we have some that were read before we got
    // the error?

    req = self.client.findObjects(common.FASTDELETE_BUCKET, '(_id>=0)', findOpts);

    req.once('error', function (err) {
        self.log.info({err: err}, 'error');

        if (common.isMorayOverloaded(err)) {
            self.log.info('moray is overloaded, we should back off');
        }

        callback(err);
    });

    req.on('record', function (obj) {
        var idx;
        var storageId;
        var value = obj.value;

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
            //

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

    req.on('end', function () {
        callback(null, {
            ids: ids,
            records: records
        });
    });
}

GarbageDirConsumer.prototype.writeGarbageInstructions = function writeGarbageInstructions(opts, callback) {
    var self = this;

    assert.object(opts, 'opts');
    //assert.object(opts.records, 'opts.records');
    //

    vasync.forEachPipeline({
        func: function _writeGarbageInstruction(storageId, cb) {
            var data = '';
            var date = new Date().toISOString().replace(/T|:/g, '-').split('.')[0];
            var dirnameFinal;
            var dirnameTemp;
            var filename;
            var filenameFinal;
            var filenameTemp;
            var idx;
            var record;

            filename = [date, self.config.instance, 'X', uuid(), 'mako', storageId].join('-');
            dirnameFinal = path.join(common.INSTRUCTION_ROOT, storageId);
            dirnameTemp = path.join(common.INSTRUCTION_ROOT, storageId + '.tmp');
            filenameFinal = path.join(dirnameFinal, filename);
            filenameTemp = path.join(dirnameTemp, filename);

            // Build the actual "instructions" content from the records
            for (idx = 0; idx < opts.records[storageId].length; idx++) {
                record = opts.records[storageId][idx];

                // XXX check/justify order here
                data += util.format('%s\t%s\t%s\t%s\t%d\n',
                    record.storageId,
                    record.ownerId,
                    record.objectId,
                    record.shard,
                    record.bytes
                );
            }

            self.log.info({data: data, dirnameTemp: dirnameTemp, dirnameFinal: dirnameFinal, records: opts.records[storageId], storageId: storageId}, 'Write Garbage');

            vasync.pipeline({
                funcs: [
                    function _mkdirTemp(_, next) {
                        fs.mkdir(dirnameTemp, function  _onMkdir(err) {
                            if (err && err.code !== 'EEXIST') {
                                next(err);
                                return;
                            }
                            next();
                        });
                    }, function _writeFileTemp(_, next) {
                        fs.writeFile(filenameTemp, data, next);
                    }, function _mkdirFinal(_, next) {
                        fs.mkdir(dirnameFinal, function  _onMkdir(err) {
                            if (err && err.code !== 'EEXIST') {
                                next(err);
                                return;
                            }
                            next();
                        });
                    }, function _mvFileFinal(_, next) {
                        fs.rename(filenameTemp, filenameFinal, next);
                    }
                ]
            }, function _writeFilePipeline(err) {
                self.log.info({filename: filenameFinal}, 'wrote file');
                cb(err);
            });
        },
        inputs: Object.keys(opts.records)
    }, function _pipelineComplete(err) {
        self.log.info({err: err}, 'wrote files');
        callback(null, {});
    });
};


GarbageDirConsumer.prototype.deleteProcessedGarbage = function deleteProcessedGarbage(opts, callback) {
    var self = this;

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

    self.client.deleteMany(common.FASTDELETE_BUCKET, filter, function _onDelete(err) {
        self.log.info({err: err}, 'deleteMany error');
        callback(err);
    });
};


// TODO setup metrics

function main() {
    var logger;

    vasync.pipeline({
        arg: {},
        funcs: [
            function _createLogger(ctx, cb) {
                logger = ctx.log = common.createLogger({
                    name: 'garbage-dir-consumer'
                });

                cb();
            }, function _loadConfig(ctx, cb) {
                common.loadConfig({log: ctx.log}, function _onConfig(err, cfg) {
                    if (!err) {
                        ctx.config = cfg;
                    }
                    logger.info({cfg: cfg}, 'CFG');
                    cb(err);
                });
            }, function _validateConfig(ctx, cb) {
                // ctx.log.info({ctx: ctx}, 'would validate config');
                common.validateConfig(ctx.config, function _onValidated(err, res) {
                    // XXX wtf
                    cb(err);
                });
            }, function _createDirConsumer(ctx, cb) {
                var childLog;
                var idx;
                var gdc;
                var shard;

                //
                // We create a separate GarbageDirConsumer instance for each
                // Moray shard this GC is assigned to manage.
                //
                for (idx = 0; idx < ctx.config.shards.length; idx++) {
                    shard = ctx.config.shards[idx].host;

                    childLog = ctx.log.child({
                        component: 'GarbageDirConsumer',
                        shard: shard
                    }),

                    gdc = new GarbageDirConsumer({
                        config: ctx.config,
                        log: childLog,
                        morayConfig: common.getMorayConfig({
                            collector: undefined, // XXX replace with collector
                            config: ctx.config,
                            log: childLog,
                            morayShard: shard
                        }),
                        shard: shard
                    });

                    gdc.start();
                }

                cb();
            }
        ]
    }, function _doneMain(err) {
        logger.info('startup complete');
    });
}

main();
