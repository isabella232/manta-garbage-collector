/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
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
//  * storageId
//  * objectId
//  * creatorId/ownerId
//  * shardId
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

var createMetricsManager = require('triton-metrics').createMetricsManager;
var restify = require('restify');
var vasync = require('vasync');

var common = require('../lib/common');
var GarbageDirConsumer = require('../lib/garbage-dir-consumer');

var elapsedSince = common.elapsedSince;
var ensureDelegated = common.ensureDelegated;

var METRICS_SERVER_PORT = 8882;
var SERVICE_NAME = 'garbage-dir-consumer';

function main() {
    var beginning;
    var logger;

    beginning = process.hrtime();

    vasync.pipeline(
        {
            arg: {},
            funcs: [
                function _createLogger(_, cb) {
                    logger = common.createLogger({
                        level: 'trace', // XXX temporary
                        name: SERVICE_NAME
                    });

                    cb();
                },
                function _loadConfig(ctx, cb) {
                    common.loadConfig({log: logger}, function _onConfig(
                        err,
                        cfg
                    ) {
                        if (!err) {
                            ctx.config = cfg;
                        }
                        cb(err);
                    });
                },
                function _validateConfig(ctx, cb) {
                    common.validateConfig(ctx.config, function _onValidated(
                        err,
                        res
                    ) {
                        cb(err);
                    });
                },
                function _ensureDelegated(_, cb) {
                    //
                    // If we don't have a delegated dataset, we're not going to do
                    // anything else since it would be dangerous to write any files
                    // locally.
                    //
                    ensureDelegated(function _delegatedResult(err, found) {
                        logger.debug(
                            {
                                err: err,
                                found: found
                            },
                            'ensureDelegated result.'
                        );

                        if (err) {
                            cb(err);
                        } else if (!found) {
                            cb(
                                new Error(
                                    'Instruction root not on delegated ' +
                                        'dataset, unsafe to continue.'
                                )
                            );
                        } else {
                            cb();
                        }
                    });
                },
                function _setupMetrics(ctx, cb) {
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
                    metricsManager.listen(cb);
                    ctx.metricsManager = metricsManager;
                },
                function _createDirConsumer(ctx, cb) {
                    var childLog;
                    var idx;
                    var gdc;
                    var shard;

                    if (ctx.config.dir_shards.length < 1) {
                        cb(new Error('No dir-style shards configured for GC.'));
                        return;
                    }

                    //
                    // We create a separate GarbageDirConsumer instance for each
                    // Moray shard this GC is assigned to manage.
                    //
                    for (idx = 0; idx < ctx.config.dir_shards.length; idx++) {
                        shard = ctx.config.dir_shards[idx].host;

                        childLog = logger.child({
                            component: 'GarbageDirConsumer',
                            shard: shard
                        });

                        gdc = new GarbageDirConsumer({
                            config: ctx.config,
                            log: childLog,
                            metricsManager: ctx.metricsManager,
                            morayConfig: common.getMorayConfig({
                                collector: ctx.metricsManager.collector,
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
        },
        function _doneMain(err) {
            logger.info(
                {
                    elapsed: elapsedSince(beginning),
                    err: err
                },
                'Startup complete.'
            );
        }
    );
}

main();
