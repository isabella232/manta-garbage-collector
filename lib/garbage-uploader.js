/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

//
// This contains the code for GarbageUploader objects which are used by the
// garbage-uploader program to process garbage collector instructions and send
// them to the appropriate storage servers.
//

var fs = require('fs');
var path = require('path');

var assert = require('assert-plus');
var restifyClients = require('restify-clients');
var vasync = require('vasync');

var common = require('../lib/common');

var elapsedSince = common.elapsedSince;
var forEachParallel = common.forEachParallel;

// NOTE: /manta_gc/* will be /manta/manta_gc/* on the storage node
var DEFAULT_INSTRUCTION_ROOT = common.INSTRUCTION_ROOT;
var DEFAULT_RUN_FREQ = 10000; // ms
var DEFAULT_SLOW_PUT_CUTOFF = 10; // see description below
var DEFAULT_SLOW_PUT_MAX_FAILURES = 10; // see description below
var DEFAULT_SLOW_PUT_DISABLE_TIME = 6 * 60 * 60 * 1000; // see description below
var MAKO_INSTRUCTION_DIR = '/manta_gc/instructions';
var METRIC_PREFIX = 'gc_uploader_';
var PROCESS_FILE_CONCURRENCY = 10; // files per zone to process at once
var PROCESS_FILE_MAX_QUEUED = 100; // files per zone to queue at once
var PROCESS_ZONE_CONCURRENCY = 5; // zones to process at once

function GarbageUploader(opts) {
    var self = this;

    assert.object(opts, 'opts');
    assert.optionalString(opts.instructionRoot, 'opts.instructionRoot');
    assert.object(opts.log, 'opts.log');
    assert.optionalNumber(opts.runFreq, 'opts.runFreq');
    assert.optionalObject(opts.metricsManager, 'opts.metricsManager');

    // Options that exist only for testing.
    assert.optionalFunc(opts._putFileHook, 'opts._putFileHook');
    assert.optionalFunc(opts._runHook, 'opts._runHook');
    assert.optionalObject(opts.storageServerMap, 'opts.storageServerMap');

    self.config = opts.config;
    self.dirtyStorageZones = {};
    self.dirMtimes = {};
    self.dirWatchers = {};
    self.disabledStorageIds = {};
    self.instructionRoot = opts.instructionRoot || DEFAULT_INSTRUCTION_ROOT;
    self.log = opts.log;
    self.metricsManager = opts.metricsManager;
    self.nextTimer = null;
    self.runFreq = opts.runFreq || DEFAULT_RUN_FREQ; // ms
    self.storageServerMap = opts.storageServerMap;
    self.storageServerPutTimes = {}; // Tracked so we can observe via core/mdb
    self.storageServerFailedSlowPuts = {};

    if (!self.config.options) {
        self.config.options = {};
    }

    //
    // self.slowPutCutoff is the number of seconds after which a failed PUT will
    // be considered to have been "slow" and to have timed out.
    //
    // self.slowPutMaxFailures is the number of consecutive slow PUT failures
    // after which a storageId will be disabled.
    //
    // self.slowPutDisableTime is the amount of time (in ms) for which to
    // disable a storageId after self.slowPutMaxFailures "slow" failures.
    //
    self.slowPutCutoff =
        self.config.options.dir_slow_put_cutoff || DEFAULT_SLOW_PUT_CUTOFF;
    self.slowPutMaxFailures =
        self.config.options.dir_slow_put_max_failures ||
        DEFAULT_SLOW_PUT_MAX_FAILURES;
    self.slowPutDisableTime =
        self.config.options.dir_slow_put_disable_time ||
        DEFAULT_SLOW_PUT_DISABLE_TIME;

    if (opts._putFileHook) {
        self._putFileHook = opts._putFileHook;
    }
    if (opts._runHook) {
        self._runHook = opts._runHook;
    }

    if (self.metricsManager) {
        self.metrics = {
            instrFileDeleteCountTotal: self.metricsManager.collector.counter({
                name: METRIC_PREFIX + 'instruction_file_delete_count_total',
                help:
                    'Counter incremented every time the uploader deletes ' +
                    'an instruction file after uploading it.'
            }),
            instrFileDeleteErrorCountTotal: self.metricsManager.collector.counter(
                {
                    name:
                        METRIC_PREFIX +
                        'instruction_file_delete_error_count_total',
                    help:
                        'Counter incremented every time the uploader fails to ' +
                        'delete an instruction file after uploading it.'
                }
            ),
            instrFileInvalidCountTotal: self.metricsManager.collector.counter({
                name: METRIC_PREFIX + 'instruction_file_invalid_count_total',
                help:
                    'Counter incremented every time the uploader encounters ' +
                    'an invalid instruction file.'
            }),
            runCountTotal: self.metricsManager.collector.counter({
                name: METRIC_PREFIX + 'run_count_total',
                help:
                    'Counter incremented every time the uploader looks for ' +
                    'new instruction files.'
            }),
            runErrorCountTotal: self.metricsManager.collector.counter({
                name: METRIC_PREFIX + 'run_error_count_total',
                help:
                    'Counter incremented every time the uploader looks for ' +
                    'new instruction files and has an error.'
            }),
            runSecondsTotal: self.metricsManager.collector.counter({
                name: METRIC_PREFIX + 'run_seconds_total',
                help:
                    'Total number of seconds spent scanning and uploading ' +
                    'instruction files'
            }),
            instrFileUploadSuccessCountTotal: self.metricsManager.collector.counter(
                {
                    name: METRIC_PREFIX + 'instruction_file_upload_count_total',
                    help:
                        'Counter incremented every time an instruction file is ' +
                        'uploaded successfully.'
                }
            ),
            instrFileUploadSecondsTotal: self.metricsManager.collector.counter({
                name: METRIC_PREFIX + 'instruction_file_upload_seconds_total',
                help:
                    'Total number of seconds spent uploading instruction ' +
                    'files'
            }),
            instrFileUploadAttemptCountTotal: self.metricsManager.collector.counter(
                {
                    name:
                        METRIC_PREFIX +
                        'instruction_file_upload_attempt_count_total',
                    help:
                        'Counter incremented every time an attempt is made to ' +
                        'upload an instruction file.'
                }
            ),
            instrFileUploadErrorCountTotal: self.metricsManager.collector.counter(
                {
                    name:
                        METRIC_PREFIX +
                        'instruction_file_upload_error_count_total',
                    help:
                        'Counter incremented every time an error occurrs ' +
                        'attempting  to upload an instruction file.'
                }
            ),
            watchedDirectoryCount: self.metricsManager.collector.gauge({
                name: METRIC_PREFIX + 'watched_directory_count',
                help:
                    'Gauge of number of directories currently being watched' +
                    ' for new instructions files'
            })
        };
    } else {
        self.metrics = {};
    }

    self.addCounter('instrFileInvalidCountTotal', 0);
    self.addCounter('instrFileUploadAttemptCountTotal', 0);
    self.addCounter('instrFileUploadErrorCountTotal', 0);
    self.addCounter('instrFileUploadSecondsTotal', 0);
    self.addCounter('instrFileUploadSuccessCountTotal', 0);
    self.addCounter('instrFileDeleteCountTotal', 0);
    self.addCounter('instrFileDeleteErrorCountTotal', 0);
    self.setGauge('watchedDirectoryCount', 0);
    self.addCounter('runCountTotal', 0);
    self.addCounter('runErrorCountTotal', 0);
    self.addCounter('runSecondsTotal', 0);
}

GarbageUploader.prototype.addCounter = common.addCounter;
GarbageUploader.prototype.getCounter = common.getCounter;
GarbageUploader.prototype.getGauge = common.getGauge;
GarbageUploader.prototype.setGauge = common.setGauge;

GarbageUploader.prototype.start = function start(callback) {
    var self = this;

    assert.optionalFunc(callback, 'callback');

    self.log.info('Starting GarbageUploader.');

    // Kick off first run. This will schedule the next run if it succeeds.
    self.run(callback);
};

GarbageUploader.prototype.stop = function stop(callback) {
    var self = this;
    var dirWatchers;
    var idx;

    assert.optionalFunc(callback, 'callback');

    self.log.info('Stopping GarbageUploader.');

    self.stopping = true;
    clearTimeout(self.nextTimer);
    self.nextTimer = null;

    dirWatchers = Object.keys(self.dirWatchers);
    for (idx = 0; idx < dirWatchers.length; idx++) {
        self.dirWatchers[dirWatchers[idx]].close();
    }

    if (callback) {
        callback();
    }
};

GarbageUploader.prototype.run = function run(callback) {
    var self = this;

    var before;
    var beginning;
    var dirtyCount;

    assert.optionalFunc(callback, 'callback');

    // Keep track of what the metrics looked like before this run so we can
    // include changes in our results.
    before = {
        instrDeletes: self.getCounter('instrFileDeleteCountTotal'),
        instrDeleteErrors: self.getCounter('instrFileDeleteErrorCountTotal'),
        instrsInvalid: self.getCounter('instrFileInvalidCountTotal'),
        uploadAttempts: self.getCounter('instrFileUploadAttemptCountTotal'),
        uploadErrors: self.getCounter('instrFileUploadErrorCountTotal'),
        uploadSuccesses: self.getCounter('instrFileUploadSuccessCountTotal')
    };

    self.log.debug({dirty: self.dirtyStorageZones}, 'Running GarbageUploader.');

    beginning = process.hrtime();
    dirtyCount = Object.keys(self.dirtyStorageZones).length;

    vasync.pipeline(
        {
            funcs: [
                function _scanInstructionDirs(_, cb) {
                    self.scanInstructionDirs(cb);
                },
                function _processDirtyStorageZones(_, cb) {
                    self.processDirtyStorageZones(cb);
                }
            ]
        },
        function _endRun(err) {
            var after;
            var logLevel = 'info';
            var resultInfo;

            self.addCounter('runCountTotal', 1);

            after = {
                instrDeleteErrors: self.getCounter(
                    'instrFileDeleteErrorCountTotal'
                ),
                instrDeletes: self.getCounter('instrFileDeleteCountTotal'),
                instrsInvalid: self.getCounter('instrFileInvalidCountTotal'),
                uploadAttempts: self.getCounter(
                    'instrFileUploadAttemptCountTotal'
                ),
                uploadErrors: self.getCounter('instrFileUploadErrorCountTotal'),
                uploadSuccesses: self.getCounter(
                    'instrFileUploadSuccessCountTotal'
                )
            };

            resultInfo = {
                dirtyCount: dirtyCount,
                elapsed: elapsedSince(beginning),
                err: err,
                instrDeleteErrors:
                    after.instrDeleteErrors - before.instrDeleteErrors,
                instrDeletes: after.instrDeletes - before.instrDeletes,
                instrsInvalid: after.instrsInvalid - before.instrsInvalid,
                uploadAttempts: after.uploadAttempts - before.uploadAttempts,
                uploadErrors: after.uploadErrors - before.uploadErrors,
                uploadSuccesses: after.uploadSuccesses - before.uploadSuccesses
            };

            self.addCounter('runSecondsTotal', resultInfo.elapsed);

            if (err) {
                logLevel = 'error';
                self.addCounter('runErrorCountTotal', 1);
            }

            // On error we'll have logged the error, but otherwise there's nothing
            // to do since we'll run again in runFreq ms anyway.
            self.log[logLevel](
                resultInfo,
                'GarbageUploader run complete. Will run again in %d seconds.',
                Math.floor(self.runFreq / 1000)
            );

            // Schedule next run.
            if (!self.stopping) {
                self.nextTimer = setTimeout(self.run.bind(self), self.runFreq);
            }

            if (self._runHook) {
                self._runHook(resultInfo);
            }

            if (callback) {
                callback(err, resultInfo);
            }
        }
    );
};

GarbageUploader.prototype.enableStorageId = function enableStorageId(
    storageId
) {
    var self = this;

    // Need a temporary variable since we're going to delete the entry when we
    // enable, but want the log to include the timestamp.
    var disableTimestamp = self.disabledStorageIds[storageId];

    if (disableTimestamp) {
        delete self.disabledStorageIds[storageId];
        self.log.debug(
            'Enabled ' +
                storageId +
                ' which was disabled at ' +
                disableTimestamp
        );
    }

    // Mark it dirty so we'll re-scan it on the next run
    self.dirtyStorageZones[storageId] = 0;
};

GarbageUploader.prototype.disableStorageId = function disableStorageId(
    storageId,
    disableTime
) {
    var self = this;

    if (self.disabledStorageIds[storageId]) {
        // already disabled
        return;
    }

    self.disabledStorageIds[storageId] = Date.now();

    // Remove from failed list so that when enabled, we'll not disable again
    // on the first failed PUT next run (since this value is currently > the
    // limit). This effectively restarts the counter at 0 for the next run.
    delete self.storageServerFailedSlowPuts[storageId];

    if (disableTime > 0) {
        //
        // If disableTime is set, we'll re-enable it that many ms in the future
        // otherwise we'll disable it for the life of this process.
        //
        // If the problems with it haven't been fixed, the most likely result
        // here will be that it will get disabled again on the next run.
        //
        setTimeout(self.enableStorageId.bind(self, storageId), disableTime);
        self.log.debug(
            'Disabled ' +
                storageId +
                ' will re-enable in ' +
                disableTime +
                ' ms'
        );
    }
};

GarbageUploader.prototype.putInstructionFile = function putInstructionFile(
    filename,
    dirname,
    storageId,
    callback
) {
    var self = this;

    var beginning;
    var client;
    var filePath;
    var remotePath;
    var remoteServer;

    assert.string(filename, 'filename');
    assert.string(dirname, 'dirname');
    assert.string(storageId, 'storageId');
    assert.func(callback, 'callback');

    filePath = path.join(dirname, filename);
    remotePath = path.join(MAKO_INSTRUCTION_DIR, filename);
    remoteServer = storageId;

    if (self.disabledStorageIds[storageId]) {
        self.log.trace('Ignoring PUT for disabled storageId: ' + storageId);
        callback();
        return;
    }

    self.log.debug(
        {
            filename: filename,
            dirname: dirname,
            remotePath: remotePath,
            storageId: storageId
        },
        'PUTing GC instruction file.'
    );

    // Allow overriding the remote hostname being StorageId (so we can use
    // different ports, e.g. for testing).
    if (self.storageServerMap && self.storageServerMap[storageId]) {
        remoteServer = self.storageServerMap[storageId];
    }

    beginning = process.hrtime();

    client = restifyClients.createClient({
        agent: false,
        connectTimeout: 5000,
        retry: {
            retries: 0
        },
        requestTimeout: 10000,
        url: 'http://' + remoteServer
    });

    if (!self.storageServerPutTimes[storageId]) {
        self.storageServerPutTimes[storageId] = 0;
    }

    function _handlePutFail(err, req, res) {
        var elapsed = elapsedSince(beginning);

        if (self._putFileHook) {
            self._putFileHook({
                err: err,
                filename: filename,
                res: res
            });
        }

        self.addCounter('instrFileUploadErrorCountTotal', 1);
        self.addCounter('instrFileUploadSecondsTotal', elapsed);
        self.storageServerPutTimes[storageId] += elapsed;

        self.log.warn(
            {
                elapsed: elapsedSince(beginning),
                err: err,
                result: res
                    ? {
                          headers: res.headers,
                          statusCode: res.statusCode,
                          statusMessage: res.statusMessage
                      }
                    : res,
                req: req
            },
            'PUT Failed. Will retry.'
        );

        if (!self.storageServerFailedSlowPuts[storageId]) {
            self.storageServerFailedSlowPuts[storageId] = 0;
        }

        if (elapsed > self.slowPutCutoff) {
            self.storageServerFailedSlowPuts[storageId]++;

            self.log.warn(
                'Slow PUT: ' +
                    elapsed +
                    's: ' +
                    self.storageServerFailedSlowPuts[storageId]
            );

            if (
                self.storageServerFailedSlowPuts[storageId] >=
                self.slowPutMaxFailures
            ) {
                self.log.warn(
                    'Too many consecutive PUT failures, disabling uploads for ' +
                        storageId
                );
                self.disableStorageId(storageId, self.slowPutDisableTime);
            }
        }
    }

    client.put(remotePath, function _onPut(err, req) {
        var elapsed = elapsedSince(beginning);
        var readStream;

        self.addCounter('instrFileUploadAttemptCountTotal', 1);

        if (err) {
            _handlePutFail(err, req);

            // We'll not unlink since we want to try this again.
            callback(err);
            return;
        }

        readStream = fs.createReadStream(filePath);
        readStream.pipe(req);

        req.once('result', function _onResult(e, res) {
            var newErr;

            readStream.destroy();

            elapsed = elapsedSince(beginning);

            self.addCounter('instrFileUploadSecondsTotal', elapsed);

            self.storageServerPutTimes[storageId] += elapsed;

            if (e) {
                _handlePutFail(e, req, res);

                // We'll not unlink since we want to try this again.
                callback(e);
                return;
            } else if (res.statusCode !== 201) {
                newErr = new Error(
                    'PUT Failed: ' + res.statusCode + ': ' + res.statusMessage
                );

                _handlePutFail(newErr, req, res);

                // We'll not unlink since we want to try this again.
                callback(newErr);
                return;
            }

            // Since we succeeded, we clear the failed put counter since we want
            // that to only trigger a disable when there are *consecutive*
            // failures.
            delete self.storageServerFailedSlowPuts[storageId];

            self.log.debug(
                {
                    elapsed: elapsedSince(beginning),
                    req: req,
                    result: {
                        headers: res.headers,
                        statusCode: res.statusCode,
                        statusMessage: res.statusMessage
                    }
                },
                'PUT Success. Deleting %s.',
                filePath
            );

            self.addCounter('instrFileUploadSuccessCountTotal', 1);

            //
            // NOTE: This unlink() will cause another event to be sent to
            //       fs.watch(). But this is fine. We'll do a new read and
            //       if all the files are gone that's ok.
            //
            //       If unlink fails (because the file is readonly or some
            //       other problem) we'll have to just hope someone notices
            //       the error counter going up. Nothing will be seriously
            //       wrong in this case, but we'll be wasting a lot of
            //       cycles deleting the same files over and over which will
            //       already have been removed on the mako.
            //

            fs.unlink(filePath, function _onUnlink(unlinkErr) {
                if (unlinkErr) {
                    self.addCounter('instrFileDeleteErrorCountTotal', 1);
                }
                self.addCounter('instrFileDeleteCountTotal', 1);

                if (self._putFileHook) {
                    self._putFileHook({
                        err: unlinkErr,
                        filename: filename,
                        res: res
                    });
                }

                callback(unlinkErr);
            });
        });
    });
};

GarbageUploader.prototype.processDirtyStorageZone = function processDirtyStorageZone(
    storageId,
    callback
) {
    var self = this;

    var beginning;
    var dir;
    var startTime;

    assert.string(storageId, 'storageId');
    assert.func(callback, 'callback');

    self.log.debug(
        {storageId: storageId},
        'Processing files for storage zone.'
    );

    if (self.disabledStorageIds[storageId]) {
        self.log.trace('Ignoring disabled storageId: ' + storageId);

        // We clear the "dirty" flag here because otherwise we'd be right back
        // here again on the next run. If the server is enabled again, it will
        // be marked dirty at that time to force a run.
        delete self.dirtyStorageZones[storageId];

        callback();
        return;
    }

    dir = path.join(self.instructionRoot, storageId);

    beginning = process.hrtime();
    startTime = new Date().getTime();

    fs.readdir(dir, function _onReaddir(err, files) {
        self.log.debug(
            {
                elapsed: elapsedSince(beginning),
                err: err,
                files: files,
                storageId: storageId
            },
            'Read file list for storage zone.'
        );

        if (err) {
            callback(err);
            return;
        }

        forEachParallel(
            {
                concurrency: PROCESS_FILE_CONCURRENCY,
                maxQueued: PROCESS_FILE_MAX_QUEUED,
                func: function _callPutter(file, cb) {
                    if (!file.match(/\.instruction$/)) {
                        self.addCounter('instrFileInvalidCountTotal', 1);

                        self.log.warn(
                            {
                                filename: file
                            },
                            'Ignoring non-instruction file.'
                        );

                        cb();
                        return;
                    }
                    self.putInstructionFile(file, dir, storageId, cb);
                },
                inputs: files
            },
            function _processedAllFiles(procErr, results) {
                // If there was an error we'll have incremented an error metric but
                // there's not much else to do since we'll try again.

                self.log.debug(
                    {
                        elapsed: elapsedSince(beginning),
                        err: procErr,
                        results: results,
                        storageId: storageId
                    },
                    'Done processing files for storage zone.'
                );

                //
                // When we (processDirtyStorageZone) started, the zone was already
                // "dirty". If a new file was added while we were running, the
                // timestamp of the self.dirtyStorageZones[] entry will be *after*
                // we started. We only clear the dirty marker if there have been no
                // events since we started running and there was no error.
                //
                if (!procErr && self.dirtyStorageZones[storageId] < startTime) {
                    self.log.trace(
                        {storageId: storageId},
                        'Clearing "dirty" flag for storage zone.'
                    );
                    delete self.dirtyStorageZones[storageId];
                }

                // If there was no error, we did all the processing for this zone.

                callback();
            }
        );
    });
};

GarbageUploader.prototype.processDirtyStorageZones = function processDirtyStorageZones(
    callback
) {
    var self = this;

    var beginning;

    assert.func(callback, 'callback');

    self.log.debug(
        {dirty: self.dirtyStorageZones},
        'Processing all "dirty" storage zones.'
    );

    beginning = process.hrtime();

    forEachParallel(
        {
            concurrency: PROCESS_ZONE_CONCURRENCY,
            func: self.processDirtyStorageZone.bind(self),
            inputs: Object.keys(self.dirtyStorageZones)
        },
        function(err, results) {
            self.log.debug(
                {
                    elapsed: elapsedSince(beginning),
                    err: err,
                    results: results
                },
                'Processed files for all "dirty" storage zones.'
            );

            callback();
        }
    );
};

GarbageUploader.prototype.scanInstructionDirs = function scanInstructionDirs(
    callback
) {
    var self = this;

    assert.func(callback, 'callback');

    vasync.pipeline(
        {
            arg: {
                instructionDirs: [],
                instructionRootModified: false
            },
            funcs: [
                function _getInstructionRootDirMtime(ctx, cb) {
                    fs.stat(self.instructionRoot, function _onStat(err, stats) {
                        var newMtime;
                        var prevMtime;

                        if (err) {
                            if (err.code === 'ENOENT') {
                                self.log.warn(
                                    'Directory %s did not exist, ignoring.',
                                    self.instructionRoot
                                );
                                cb();
                            } else {
                                cb(err);
                            }
                            return;
                        }

                        assert.object(stats, 'stats');
                        assert.object(stats.mtime, 'stats.mtime');

                        newMtime = stats.mtime.getTime();
                        prevMtime = self.dirMtimes[self.instructionRoot] || 0;

                        if (newMtime > prevMtime) {
                            self.log.info(
                                {
                                    prevMtime: prevMtime,
                                    mtime: newMtime,
                                    dir: self.instructionRoot
                                },
                                'Instruction root has new mtime. Will reload.'
                            );
                            self.dirMtimes[self.instructionRoot] = newMtime;
                            ctx.instructionRootModified = true;
                        }

                        cb();
                    });
                },
                function _checkInstructionRootDir(ctx, cb) {
                    if (!ctx.instructionRootModified) {
                        self.log.debug(
                            'Instruction root not modified. No need to reload root dirs.'
                        );
                        cb();
                        return;
                    }

                    fs.readdir(self.instructionRoot, function _readDirs(
                        readErr,
                        files
                    ) {
                        var dirs;

                        if (readErr) {
                            cb(readErr);
                            return;
                        }

                        dirs = files.filter(function _isStorDir(dir) {
                            if (
                                dir.match(/^[0-9]+\.stor\./) &&
                                !dir.match(/\.tmp$/)
                            ) {
                                return true;
                            }
                            return false;
                        });

                        ctx.instructionDirs = dirs;
                        self.log.trace({dirs: dirs}, 'Found directories.');
                        cb();
                    });
                },
                function _setupMissingWatchers(ctx, cb) {
                    var dir;
                    var idx;
                    var storageId;

                    for (idx = 0; idx < ctx.instructionDirs.length; idx++) {
                        dir = path.join(
                            self.instructionRoot,
                            ctx.instructionDirs[idx]
                        );
                        storageId = ctx.instructionDirs[idx];

                        if (!self.dirWatchers.hasOwnProperty(dir)) {
                            //
                            // We need to make a closure around dir and storageId
                            // here since otherwise the event handler would not have
                            // them available when it fires.
                            //
                            // eslint-disable-next-line no-inner-declarations
                            function _makeEventHandler(_dir, _storageId) {
                                return function _onEvent(_event) {
                                    if (!self.disabledStorageIds[_storageId]) {
                                        self.dirtyStorageZones[
                                            _storageId
                                        ] = new Date().getTime();
                                    }
                                    self.log.trace(
                                        {
                                            ignored: !!self.disabledStorageIds[
                                                _storageId
                                            ]
                                        },
                                        'fs.watch event: %s: %s',
                                        _dir,
                                        _event
                                    );
                                }.bind(self);
                            }

                            self.log.info(
                                'Adding watcher for directory "%s".',
                                dir
                            );
                            self.dirWatchers[dir] = fs.watch(
                                dir,
                                {persistent: false},
                                _makeEventHandler(dir, storageId)
                            );

                            // Keep track of # of watched directories
                            self.setGauge(
                                'watchedDirectoryCount',
                                Object.keys(self.dirWatchers).length
                            );

                            //
                            // Mark it "dirty" initially since it might already have
                            // files in it before we setup our watcher. If it
                            // doesn't we'll find that out when we readdir it the
                            // first time and there just won't be files to process.
                            //
                            self.dirtyStorageZones[
                                ctx.instructionDirs[idx]
                            ] = 0;
                        }
                    }

                    cb();
                }
            ]
        },
        function _scannedInstructionDirs(err) {
            callback(err);
        }
    );
};

module.exports = GarbageUploader;
