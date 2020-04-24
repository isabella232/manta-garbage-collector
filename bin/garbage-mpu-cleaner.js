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
//   manta_uploads
//
// Moray bucket(s) for a directory-based Manta installation, and for each
// completed upload record found, it will attempt to delete the associated
// intermediate MPU parts and directory from the "manta" Moray bucket.
//
//
// Config Options: (TODO explain what each of these does and how set)
//
//  * admin_ip
//  * datacenter
//  * dir_shards (array of objects with a 'host' (DNS hostname) property)
//  * instance
//  * emoray (emoray.options.cueballOptions.resolvers, emoray.options.srvDomain)
//  * moray (moray.options.cueballOptions.resolvers)
//  * mpu_cleanup_age_seconds
//  * mpu_cleanup_batch_interval_ms
//  * mpu_cleanup_batch_size
//  * server_uuid
//

var createMetricsManager = require('triton-metrics').createMetricsManager;
var restify = require('restify');
var vasync = require('vasync');

var common = require('../lib/common');
var GarbageMpuCleaner = require('../lib/garbage-mpu-cleaner');
var GarbageMpuPartsCleaner = require('../lib/garbage-mpu-parts-cleaner');

var elapsedSince = common.elapsedSince;

var METRIC_PREFIX = 'gc_mpu_cleaner_';
var METRICS_SERVER_PORT = 8884;
var SERVICE_NAME = 'garbage-mpu-cleaner';

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
                        level: 'info',
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
                        err
                    ) {
                        cb(err);
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
                function _createMpuPartsCleaner(ctx, cb) {
                    //
                    // We want just *one* connection to electric-moray, so we
                    // create that here. The GarbageMpuCleaners call the
                    // metadataDeleter function from this and that will add them
                    // to the queue to be processed. When processing for the
                    // batch is complete, the callback will be called so the
                    // GarbageMpuCleaner instance knows it can remove those
                    // manta_upload records which succeeded. (we'll pass them
                    // back by _id).
                    //
                    var childLog;

                    childLog = logger.child(
                        {component: 'GarbageMpuPartsCleaner'},
                        true
                    );

                    ctx.mpc = new GarbageMpuPartsCleaner({
                        config: ctx.config,
                        log: childLog,
                        metricPrefix: METRIC_PREFIX,
                        metricsManager: ctx.metricsManager,
                        emorayConfig: common.getEMorayConfig({
                            collector: ctx.metricsManager.collector,
                            config: ctx.config,
                            log: childLog
                        })
                    });

                    ctx.mpc.start(cb);
                },
                function _createMpuCleaners(ctx, cb) {
                    var childLog;
                    var idx;
                    var gmc;
                    var shard;
                    var shards = [];

                    if (ctx.config.dir_shards.length < 1) {
                        cb(new Error('No dir-style shards configured for GC.'));
                        return;
                    }

                    //
                    // We create a separate GarbageMpuCleaner instance for each
                    // Moray shard this GC is assigned to manage.
                    //
                    for (idx = 0; idx < ctx.config.dir_shards.length; idx++) {
                        shard = ctx.config.dir_shards[idx].host;

                        childLog = logger.child(
                            {
                                component: 'GarbageMpuCleaner',
                                shard: shard
                            },
                            true
                        );

                        gmc = new GarbageMpuCleaner({
                            config: ctx.config,
                            log: childLog,
                            metadataDeleter: ctx.mpc.metadataDeleter.bind(
                                ctx.mpc
                            ),
                            metricPrefix: METRIC_PREFIX,
                            metricsManager: ctx.metricsManager,
                            morayConfig: common.getMorayConfig({
                                collector: ctx.metricsManager.collector,
                                config: ctx.config,
                                log: childLog,
                                morayShard: shard
                            }),
                            shard: shard
                        });

                        gmc.start();

                        shards.push(gmc);
                    }

                    ctx.metricsManager.collector
                        .gauge({
                            name: METRIC_PREFIX + 'moray_shard_count',
                            help:
                                'Number of Moray shards from which this ' +
                                'garbage-mpu-cleaner instance is cleaning.'
                        })
                        .set(shards.length);

                    cb();
                }
            ]
        },
        function _doneMain(err) {
            if (err) {
                logger.error(
                    {elapsed: elapsedSince(beginning), err: err},
                    'Startup failed.'
                );
                return;
            }
            logger.info(
                {elapsed: elapsedSince(beginning)},
                'Startup complete.'
            );
        }
    );
}

main();
