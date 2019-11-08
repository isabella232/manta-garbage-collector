/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2019 Joyent, Inc.
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
//  * none yet
//
//
// TODO:
//
//  * Test that mako being down and coming back does the right thing
//  * Test that adding new mako to collection works.
//  * When can't unlink, what should we do? Not send again for a while?
//  * When PUT fails, what should we do?
//  * Should we calculate the MD5 so we can compare to res.headers.x-joyent-computed-content-md5?
//  * Metric: would like to know how frequently we're seeing files show up?
//  * need to timeout if can't finish send for a mako
//  * does fs.readdir fail with too many files in a directory? Probably.
//      + https://github.com/nodejs/node/pull/29349
//
//
// Future:
//
//  If fs.readdir() is slow, we can fs.stat() each time we load a directory,
//  then we can fs.stat() before doing fs.readdir() when doing periodic scan and
//  only scan when mtime > prev.
//
//  Add support for backing off broken Makos.
//
//  If we need files for "new" makos to show up faster (instead of just on the
//  next loop) we can add a fs.watch() watcher for the instruction root dir.
//

var fs = require('fs');
var path = require('path');

var assert = require('assert-plus');
var createMetricsManager = require('triton-metrics').createMetricsManager;
var restify = require('restify');
var restifyClients = require('restify-clients');
var triton_metrics = require('triton-metrics');
var vasync = require('vasync');

var common = require('../lib/common');

var elapsedSince = common.elapsedSince;
// NOTE: /manta_gc/* will be /manta/manta_gc/* on the storage node
var MAKO_INSTRUCTION_DIR = '/manta_gc/instructions';
var METRICS_SERVER_PORT = 8883;
var SERVICE_NAME = 'garbage-uploader';


function GarbageUploader(opts) {
    var self = this;

    // XXX assert params

    self.config = opts.config;
    self.dirtyStorageZones = {};
    self.dirMtimes = {};
    self.dirWatchers = {};
    self.filesDeletedCounts = {};
    self.filesDeletedTotal = 0;
    self.filesReadCounts = {};
    self.filesReadTotal = 0;
    self.log = opts.log;
    self.metricsManager = opts.metricsManager;
    self.nextTimer = null;
    self.runFreq = 10000; // ms
}


/*
 Assumptions:

  * new directories will show up, but we don't care if they disappear (without restart)
  *

*/

GarbageUploader.prototype.start = function start(callback) {
    var self = this;

    assert.optionalFunc(callback, 'callback');

    self.log.info('Starting GarbageUploader.');

    // Kick off first run. This will schedule the next run if it succeeds.
    self.run(callback);
};


GarbageUploader.prototype.stop = function stop(callback) {
    var self = this;

    assert.optionalFunc(callback, 'callback');

    self.log.info('Stopping GarbageUploader.');

    if (self.nextTimer !== null) {
        clearTimeout(self.nextTimer);
        self.nextTimer = null;
    }

    if (callback) {
        callback()
    }
};


GarbageUploader.prototype.run = function run(callback) {
    var self = this;

    var beginning;
    var beforeDeletedCount;
    var beforeReadCount;
    var dirtyCount;

    assert.optionalFunc(callback, 'callback');

    self.log.debug({dirty: self.dirtyStorageZones}, 'Running GarbageUploader.');

    beginning = process.hrtime();
    beforeDeletedCount = self.filesDeletedTotal;
    beforeReadCount = self.filesReadTotal;
    dirtyCount = Object.keys(self.dirtyStorageZones).length;

    vasync.pipeline({
        funcs: [
            function _scanInstructionDirs(_, cb) {
                self.scanInstructionDirs(cb);
            }, function _processDirtyStorageZones(_, cb) {
                self.processDirtyStorageZones(cb);
            }
        ]
    }, function _endRun(err) {
        var runDeleted = self.filesDeletedTotal - beforeDeletedCount;
        var runRead = self.filesReadTotal - beforeReadCount;

        self.log.info({
            deleted: runDeleted,
            dirtyCount: dirtyCount,
            elapsed: elapsedSince(beginning),
            err: err,
            read: runRead
        }, 'GarbageUploader run complete.');

        // XXX if there's an error should we do anything other than log it?

        // Schedule next run.
        self.nextTimer = setTimeout(self.run.bind(self), self.runFreq);

        if (callback) {
            callback();
        }
    });
};


GarbageUploader.prototype.putInstructionFile = function putInstructionFile(filename, dirname, storageId, callback) {
    var self = this;

    var beginning;
    var client;
    var filePath;
    var remotePath;

    assert.string(filename, 'filename');
    assert.string(dirname, 'dirname');
    assert.string(storageId, 'storageId');
    assert.func(callback, 'callback');

    self.log.debug({filename: filename, dirname: dirname, storageId: storageId},
        'PUTing GC instruction file.');

    filePath = path.join(dirname, filename);
    remotePath = path.join(MAKO_INSTRUCTION_DIR, filename);

    beginning = process.hrtime();

    client = restifyClients.createClient({
        url: 'http://' + storageId
    });

    client.put(remotePath, function _onPut(err, req) {
        fs.createReadStream(filePath).pipe(req);
        req.once('result', function _onResult(e, res) {
            if (!err) {
                if (res.statusCode !== 201) {
                    self.log.warn({
                        elapsed: elapsedSince(beginning),
                        result: {
                            headers: res.headers,
                            statusCode: res.statusCode,
                            statusMessage: res.statusMessage
                        },
                        req: req
                    }, 'PUT Failed. Will retry.');

                    callback(new Error('PUT Failed: %s: %s',
                        res.statusCode,
                        res.statusMessage));
                    return;
                }

                self.log.debug({
                    elapsed: elapsedSince(beginning),
                    req: req,
                    result: {
                        headers: res.headers,
                        statusCode: res.statusCode,
                        statusMessage: res.statusMessage
                    }
                }, 'PUT Success. Deleting %s.', filePath);

                // XXX what happens when unlink fails (readonly, etc)
                //
                // NOTE: This unlink() will cause another event to be sent to
                //       fs.watch(). But this is fine. We'll do a new read and
                //       if all the files are gone that's ok.
                fs.unlink(filePath);

                if (!self.filesDeletedCounts.hasOwnProperty(storageId)) {
                    self.filesDeletedCounts[storageId] = 0;
                }
                self.filesDeletedCounts[storageId]++;
                self.filesDeletedTotal++;
            } else {
                self.log.warn({
                    elapsed: elapsedSince(beginning),
                    err: e,
                    req: req
                }, 'PUT Failed. Will retry.');
            }

            callback(err);
        });
    });
};


GarbageUploader.prototype.processDirtyStorageZone = function processDirtyStorageZone(storageId, callback) {
    var self = this;

    var beginning;
    var dir;
    var startTime;

    assert.string(storageId, 'storageId');
    assert.func(callback, 'callback');

    self.log.debug({storageId: storageId}, 'Processing files for storage zone.');

    dir = path.join(common.INSTRUCTION_ROOT, storageId);

    beginning = process.hrtime();
    startTime = new Date().getTime();

    fs.readdir(dir, function _onReaddir(err, files) {
        self.log.debug({
            elapsed: elapsedSince(beginning),
            err: err,
            files: files,
            storageId: storageId
        }, 'Read file list for storage zone.');

        if (err) {
            callback(err);
            return;
        }

        if (!self.filesReadCounts.hasOwnProperty(storageId)) {
            self.filesReadCounts[storageId] = 0;
        }
        self.filesReadCounts[storageId] += files.length;
        self.filesReadTotal += files.length;

        vasync.forEachParallel({
            func: function _callPutter(file, cb) {
                if (!file.match(/\.instruction$/)) {
                    self.log.warn({
                        filename: file
                    }, 'Ignoring non-instruction file');
                    cb();
                    return;
                }
                self.putInstructionFile(file, dir, storageId, cb);
            },
            inputs: files
        }, function _processedAllFiles(err, results) {
            // XXX on error what to do?

            self.log.debug({
                elapsed: elapsedSince(beginning),
                err: err,
                results: results
            }, 'Done processing files for storage zone.');

            //
            // When we (processDirtyStorageZone) started, the zone was already
            // "dirty". If a new file was added while we were running, the
            // timestamp of the self.dirtyStorageZones[] entry will be *after*
            // we started. We only clear the dirty marker if there have been no
            // events since we started running and there was no error.
            //
            if (!err && self.dirtyStorageZones[storageId] < startTime) {
                self.log.trace({storageId: storageId},
                    'Clearing "dirty" flag for storage zone.');
                delete self.dirtyStorageZones[storageId];
            }

            // If there was no error, we did all the processing for this zone.

            callback();
        });
    });
};


GarbageUploader.prototype.processDirtyStorageZones = function processDirtyStorageZones(callback) {
    var self = this;

    var beginning;

    assert.func(callback, 'callback');

    self.log.debug({dirty: self.dirtyStorageZones}, 'Processing all "dirty" storage zones.');

    beginning = process.hrtime();

    vasync.forEachParallel({
        func: self.processDirtyStorageZone.bind(self),
        inputs: Object.keys(self.dirtyStorageZones)
    }, function (err, results) {
        self.log.debug({
            elapsed: elapsedSince(beginning),
            err: err,
            results: results
        }, 'Processed files for all "dirty" storage zones.');

        callback();
    });
};


GarbageUploader.prototype.scanInstructionDirs = function scanInstructionDirs(callback) {
    var self = this;

    assert.func(callback, 'callback');

    vasync.pipeline({
        arg: {
            instructionDirs: [],
            instructionRootModified: false
        },
        funcs: [
            function _getInstructionRootDirMtime(ctx, cb) {
                fs.stat(common.INSTRUCTION_ROOT, function _onStat(err, stats) {
                    var newMtime;
                    var prevMtime;

                    if (err) {
                        if (err.code === 'ENOENT') {
                            self.log.warn('Directory %s did not exist, ignoring.', common.INSTRUCTION_ROOT);
                            cb();
                        } else {
                            cb(err);
                        }
                        return;
                    }

                    assert.object(stats, 'stats');
                    assert.object(stats.mtime, 'stats.mtime');

                    newMtime = stats.mtime.getTime()
                    prevMtime = self.dirMtimes[common.INSTRUCTION_ROOT] || 0;

                    if (newMtime > prevMtime) {
                        self.log.info({prevMtime: prevMtime, mtime: newMtime, dir: common.INSTRUCTION_ROOT},
                            'Instruction root has new mtime. Will reload.');
                        self.dirMtimes[common.INSTRUCTION_ROOT] = newMtime;
                        ctx.instructionRootModified = true;
                    }

                    cb();
                });
            }, function _checkInstructionRootDir(ctx, cb) {
                if (!ctx.instructionRootModified) {
                    self.log.debug('Instruction root not modified. No need to reload root dirs.');
                    cb();
                    return;
                }

                fs.readdir(common.INSTRUCTION_ROOT, function _readDirs(err, files) {
                    var dirs;

                    dirs = files.filter(function _isStorDir(dir) {
                        if (dir.match(/^[0-9]+\.stor\./) && !dir.match(/\.tmp$/)) {
                            return true;
                        }
                        return false;
                    });

                    ctx.instructionDirs = dirs;
                    self.log.trace({dirs: dirs}, 'Found directories.');
                    cb();
                });
            }, function _setupMissingWatchers(ctx, cb) {
                var dir;
                var idx;
                var storageId;

                for (idx = 0; idx < ctx.instructionDirs.length; idx++) {
                    dir = path.join(common.INSTRUCTION_ROOT, ctx.instructionDirs[idx]);
                    storageId = ctx.instructionDirs[idx];

                    if (!self.dirWatchers.hasOwnProperty(dir)) {
                        //
                        // We need to make a closure around dir and storageId
                        // here since otherwise the event handler would not have
                        // them available when it fires.
                        //
                        function _makeEventHandler(_dir, _storageId) {
                            return function _onEvent(_event) {
                                self.dirtyStorageZones[_storageId] = new Date().getTime();
                                self.log.trace('fs.watch event: %s: %s', _dir, _event);
                            }.bind(self);
                        }

                        self.log.info('Adding watcher for directory "%s".', dir);
                        self.dirWatchers[dir] =
                            fs.watch(dir, _makeEventHandler(dir, storageId));

                        //
                        // Mark it "dirty" initially since it might already have
                        // files in it before we setup our watcher. If it
                        // doesn't we'll find that out when we readdir it the
                        // first time and there just won't be files to process.
                        //
                        self.dirtyStorageZones[ctx.instructionDirs[idx]] = 0;
                    }
                }

                cb();
            }
        ]
    }, function _scannedInstructionDirs(err) {
        callback(err);
    });
};


function main() {
    var beginning;
    var logger;

    beginning = process.hrtime();

    vasync.pipeline({
        arg: {},
        funcs: [
            function _createLogger(_, cb) {
                logger = common.createLogger({
                    level: 'trace', // XXX temporary
                    name: SERVICE_NAME
                });

                cb();
            }, function _loadConfig(ctx, cb) {
                common.loadConfig({log: logger}, function _onConfig(err, cfg) {
                    if (!err) {
                        ctx.config = cfg;
                    }
                    cb(err);
                });
            }, function _validateConfig(ctx, cb) {
                common.validateConfig(ctx.config, function _onValidated(err, res) {
                    cb(err);
                });
            }, function _setupMetrics(ctx, cb) {
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

                // TODO: setup other metrics

                metricsManager.listen(cb);

                ctx.metricsManager = metricsManager;
            }, function _createUploader(ctx, cb) {
                gu = new GarbageUploader({
                    config: ctx.config,
                    log: logger,
                    metricsManager: ctx.metricsManager
                });

                gu.start(cb);
            }
        ]
    }, function _doneMain(err) {
        logger.info({
            elapsed: elapsedSince(beginning),
            err: err
        }, 'Startup complete.');
    });
}

main();
