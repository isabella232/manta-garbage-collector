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
//  * manta_garbage
//
// and write them to "instructions" files in:
//
//   /var/spool/manta_gc/moray/<morayShardId>/<storageId>/*
//
// files where they will later be picked up by the garbage-transfer program.
//


var fs = require('fs');
var path = require('path');

var assert = require('assert-plus');
var bunyan = require('bunyan');
var createMetricsManager = require('triton-metrics').createMetricsManager;
var jsprim = require('jsprim');
var moray = require('moray');
var uuid = require('uuid/v4');
var vasync = require('vasync');
var verror = require('verror');
var VError = verror.VError;

// XXX rename this common before I die
var common = require('../lib/common.new');


function GarbageBufferer(opts) {
    var self = this;

    // XXX assert params

    var morayConfig;

    self.shard = opts.shard.host; // XXX
    self.config = opts.config;
    self.log = opts.log.child({
        component: 'GarbageBufferer',
        shard: self.shard
    });

    self.log.info({opts: opts}, 'new GarbageBufferer');

    self.lastBuffering = 0;

    self.morayConfig = common.getMorayConfig({
        collector: undefined, // XXX replace with collector
        config: self.config,
        log: self.log,
        morayShard: self.shard
    });

    // XXX error?!
    self.log.error({morayConfig: self.morayConfig}, 'moray config');
}

GarbageBufferer.prototype.bufferInstructions = function bufferInstructions() {
    var self = this;

    var beginning = process.hrtime();
    var timers = {};

    self.log.info('buffering');
    self.lastBuffering = Date.now();

    vasync.pipeline({
        arg: {},
        funcs: [
            function _findLoad(ctx, cb) {
                findGarbageLoad({
                    log: self.log,
                    morayClient: self.client,
                    startId: self.bufferedToId ? (self.bufferedToId + 1) : 0
                }, function _onGarbageLoad(err, garbageLoad) {
                    if (!err) {
                        ctx.load = garbageLoad;
                    }

                    timers.findGarbageLoad = common.elapsedSince(beginning);
                    cb(err);
                });
            }, function _bufferLoad(ctx, cb) {
                bufferGarbageLoad({
                    instance: self.config.instance,
                    log: self.log,
                    morayShard: self.shard
                }, ctx.load, function _onGarbageLoadBuffered(err) {
                    timers.bufferGarbageLoad =
                        common.elapsedSince(beginning, timers.findGarbageLoad);

                    if (!err && ctx.load.maxId > 0) {
                        self.bufferedToId = ctx.load.maxId;
                    }

                    cb(err);
                });
            }
        ]
    }, function _buffered(err) {
        self.log.info({
            err: err,
            timers: timers
        }, 'buffered!');

        // XXX if the last load was a "full load" do we want to schedule sooner?
        //     and if the last load was "empty" do we schedule later?
        self.scheduleNextBuffering();
    });
};

GarbageBufferer.prototype.scheduleNextBuffering = function scheduleNextBuffering() {
    var self = this;

    if (self.lastBuffering === 0) {
        self.bufferInstructions();
    } else {
        setTimeout(self.bufferInstructions.bind(self), 10000);
    }
};

GarbageBufferer.prototype.start = function start(callback) {
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
            }, function _scheduleFirstBuffering(ctx, cb) {
                self.scheduleNextBuffering();
                cb();
            }
        ]
    }, function _started(err) {
        self.log.info({err: err}, 'started!');
    });
};

function findGarbageLoad(opts, callback) {
    var counters = {
        totalBytes: 0,
        totalBytesWithCopies: 0,
        totalObjects: 0,
        totalObjectsByCopies: {},
        totalObjectsWithCopies: 0
    };
    var filter;
    var findOpts = {};
    var log = opts.log;
    var maxId = 0;
    var morayClient = opts.morayClient;
    var objectCount = 0;
    var startId = opts.startId;
    var records = [];
    var req;

    findOpts.limit = 1000; // XXX batch size
    findOpts.sort = {
        attribute: '_id',
        order: 'ASC'
    };
    filter = '(_id>=' + startId + ')';

    log.info({
        bucket: common.MANTA_GARBAGE_BUCKET_NAME,
        filter:filter,
        findOpts: findOpts
    }, 'findObjects');

    req = morayClient.findObjects(common.MANTA_GARBAGE_BUCKET_NAME, filter,
        findOpts);

    req.once('error', function (err) {
        var cause;

        log.info({err: err}, 'error');

        if (common.isMorayOverloaded(err)) {
            log.info('moray is overloaded, we should back off');
        }

        callback(err);
    });

    req.on('record', function (obj) {
        var idx;
        var record;
        var value = obj.value;

        // XXX verify obj is a good obj
        //     assert value.type === 'object'?
        //
        objectCount = obj._count;

        if (obj._id >= maxId) {
            maxId = obj._id;
        } else {
            log.error({oldId: maxId, newId: obj._id}, '_id went backward?!');
        }

        counters.totalBytes += value.bytes;
        counters.totalObjects++;

        // XXX obj has a '_count' property. Can we use that to determine whether
        // we should do another get right away? I.e. if count > results?

        record = {
            bytes: value.bytes,
            objectId: value.objectId,
            ownerId: value.ownerId,
            storageId: value.storageId,
        };

        records.push(record);
    });

    req.on('end', function () {
        log.info({
            counters: counters,
            maxId: maxId,
            objectCount: objectCount,
            records: records.length
        }, 'end');

        callback(null, {
            counters: counters,
            maxId: maxId,
            records: records
        });
    });
}

function makeFilename(opts) {

    /* TODO: assert
    opts.instance
    opts.morayShard
    opts.storageId
    opts.maxId
    */


    var filename;

    /*
     * XXX Make this make sense. HA!
     *
     * We maintain naming compatibility with the offline GC process here.
     * Formerly, Mako instructions were uploaded as objects to the following
     * path in manta.
     *
     * /poseidon/stor/manta_gc/mako/<manta-storage-id>/
     *  $NOW-$MARLIN_JOB-X-$UUID-mako-$MANTA_STORAGE_ID
     *
     * Where NOW=$(date +%Y-%m-%d-%H-%M-%S), $MARLIN-JOB was the jobId of
     * the marlin job that processed the database dumps leading to the
     * creation of those instructions, and UUID was a UUID generated by that
     * jobs reducer.
     *
     * The mako_gc.sh script that processes these instructions does not rely
     * on the $MARLIN_JOB or $UUID variables, so we are free to embed them
     * with our own semantics. The closest analogy to the MARLIN_JOB is the
     * zonename we're executing in. We generate a UUID for each new batch of
     * instruction we generate.
     */
    var date = new Date().toISOString().replace(/T|:/, '-').split('.')[0];

    var filename = [date, opts.maxId, opts.instance, 'X', uuid(), 'mako',
        opts.storageId].join('-');

    return path.join('/var/spool/manta_gc/by_shard',
        opts.morayShard,
        opts.storageId,
        filename);
};


function bufferGarbageLoad(opts, load, callback) {
    // XXX TODO validate params
    var batches = {};
    var filename;
    var idx = 0;
    var log = opts.log;
    var record;
    var storageId;
    var storageIds;

    if (load.records.length === 0) {
        log.info('no records found, skipping buffer');
        callback();
        return;
    }

    for (idx = 0; idx < load.records.length; idx++) {
        record = load.records[idx];

        if (!batches.hasOwnProperty(record.storageId)) {
            batches[record.storageId] = [];
        }

        batches[record.storageId].push([
            'mako',
            record.storageId,
            record.ownerId,
            record.objectId,
            record.bytes
        ].join('\t'));
    }

    log.info({batches: batches}, 'batches');

    // XXX do we still need to buffer these in memory until we hit a minimum
    // batch size?

    storageIds = Object.keys(batches);
    for (idx = 0; idx < storageIds.length; idx++) {
        storageId = storageIds[idx];

        filename = makeFilename({
            instance: opts.instance,
            morayShard: opts.morayShard,
            storageId: storageId,
            maxId: load.maxId
        });

        log.info({
            data: batches[storageId].join('\n') + '\n',
            filename: filename
        }, 'would write');
    }

    callback();
}

// TODO setup metrics

function main() {
    var logger;

    vasync.pipeline({
        arg: {},
        funcs: [
            function _createLogger(ctx, cb) {
                logger = ctx.log = common.createLogger({
                    name: 'garbage-buffer'
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
            }, function _createBufferers(ctx, cb) {
                var idx;
                var gb;

                for (idx = 0; idx < ctx.config.shards.length; idx++) {
                    gb = new GarbageBufferer({
                        config: ctx.config,
                        log: ctx.log,
                        shard: ctx.config.shards[idx]
                    });
                    gb.start();
                }

                cb();
            }
        ]
    }, function _doneMain(err) {
        logger.info('startup complete');
    });
}

main();
