/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

//
// This program exists to consume files from the:
//
//  * /var/spool/manta_gc/<storageId>/*
//
// directories and send them to the storage zones (mako) for processing. Once
// the files have been accepted by the appropriate storage zone, they will be
// deleted locally.
//
//
// Config Options:
//
//  * admin_ip
//  * datacenter
//  * instance
//  * server_uuid
//
//
// Assumptions:
//
//  * new directories will show up, but we don't care if they disappear (without
//    restart)
//
//
// Known issues:
//
//  - When we can't unlink an instruction, we'll just retry again next time.
//    It's possible we should change this so we put the file on a blacklist for
//    some amount of time, or otherwise avoid resending the same file over and
//    over.
//
//  - If we have too many files in a dir we might run out of memory on
//    fs.readdir. The best solution to this would be to use node 12+ so that we
//    can have fs.opendir. In the meantime if it gets too big we'll just start
//    crashing when trying to read the directory, which we hopefully will
//    notice. To recover from this, the mako should be fixed so it accepts
//    files again (it must have been backed up if our queue is that big) and
//    then we can move some of the files out of the dir, catch up and then move
//    the remaining files back in. Not ideal, but should work.
//
//
// Things for future consideration:
//
//  - Make concurrency/etc tunable via config file.
//
//  - Add support for backing off broken Makos?
//
//  - If we need files for "new" makos to show up faster (instead of just on the
//    next loop) we can add a fs.watch() watcher for the instruction root dir.
//    Seems unlikely to be worth it.
//
//  - There are a few things that would be nice for metrics but would blow up
//    cardinality and therefore probably are not going to happen. Such as:
//
//     - histogram for upload times
//     - storageId label on relevant metrics
//

var createMetricsManager = require('triton-metrics').createMetricsManager;
var restify = require('restify');
var vasync = require('vasync');

var common = require('../lib/common');
var GarbageUploader = require('../lib/garbage-uploader');

var elapsedSince = common.elapsedSince;

var METRICS_SERVER_PORT = 8883;
var SERVICE_NAME = 'garbage-uploader';

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
                function _createUploader(ctx, cb) {
                    var gu = new GarbageUploader({
                        config: ctx.config,
                        log: logger,
                        metricsManager: ctx.metricsManager
                    });

                    gu.start(cb);
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

if (require.main === module) {
    main();
}

module.exports = GarbageUploader;
