/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

var fs = require('fs');
var path = require('path');
var util = require('util');

var assert = require('assert-plus');
var bunyan = require('bunyan');
var forkExecWait = require('forkexec').forkExecWait;
var jsprim = require('jsprim');
var uuid = require('uuid/v4');
var vasync = require('vasync');
var verror = require('verror');
var VError = verror.VError;


var CONFIG_FILE = '/opt/smartdc/manta-garbage-collector/etc/config.json';
var FASTDELETE_BUCKET = 'manta_fastdelete_queue';
var INSTRUCTION_ROOT = '/var/spool/manta_gc';
var NS_PER_SEC = 1e9;


function createLogger(opts) {
    assert.object(opts, 'opts');
    assert.string(opts.name, 'opts.name');

    var logger = bunyan.createLogger({
        level: opts.level || process.env.LOG_LEVEL || bunyan.INFO,
        name: opts.name,
        serializers: bunyan.stdSerializers
    });

    return logger;
}

function loadConfig(opts, callback) {
    assert.object(opts, 'opts');
    assert.object(opts.log, 'opts.log');

    var parsed = {};

    if (!opts.config && !opts.filename) {
        opts.filename = CONFIG_FILE;
    }

    if (!opts.filename) {
        callback(null, parsed);
        return;
    }

    opts.log.trace(opts, 'Loading config from file.');

    fs.readFile(opts.filename, function _onReadFile(err, data) {
        if (!err) {
            try {
                parsed = JSON.parse(data.toString('utf8'));
            } catch (e) {
                callback(e);
                return;
            }
        }

        callback(err, parsed);
        return;
    });
}

function validateConfig(opts, callback) {
    // XXX TODO
    callback(null, {});
}

function getBucketsMdapiConfig(opts) {
    assert.object(opts, 'opts');
    assert.optionalObject(opts.collector, 'opts.collector');
    assert.object(opts.config, 'opts.config');
    assert.object(opts.config.buckets_mdapi, 'opts.config.buckets_mdapi');
    assert.object(opts.config.buckets_mdapi.options, 'opts.config.buckets_mdapi.options');
    assert.object(opts.log, 'opts.log');
    assert.string(opts.bucketsMdapiShard, 'opts.bucketsMdapiShard');

    var bucketsMdapiConfig = jsprim.mergeObjects(
        opts.config.buckets_mdapi.options,
        {
            collector: opts.collector,
            crc_mode: 3,
            cueballOptions: {
                maximum: 1,
                spares: 1,
                target: 1
            },
            log: opts.log,
            srvDomain: opts.bucketsMdapiShard
        }, null);

    return bucketsMdapiConfig;
}

function getMorayConfig(opts) {
    assert.object(opts, 'opts');
    assert.optionalObject(opts.collector, 'opts.collector');
    assert.object(opts.config, 'opts.config');
    assert.object(opts.config.moray, 'opts.config.moray');
    assert.object(opts.config.moray.options, 'opts.config.moray.options');
    assert.object(opts.log, 'opts.log');
    assert.string(opts.morayShard, 'opts.morayShard');

    var morayConfig = jsprim.mergeObjects(
        opts.config.moray.options,
        {
            collector: opts.collector,
            cueballOptions: {
                maximum: 1,
                spares: 1,
                target: 1
            },
            log: opts.log,
            srvDomain: opts.morayShard
        }, null);

    return morayConfig;
}

function elapsedSince(beginning, prev) {
    var elapsed;
    var timeDelta;

    timeDelta = process.hrtime(beginning);
    elapsed = timeDelta[0] + (timeDelta[1] / NS_PER_SEC);

    if (prev) {
        elapsed -= prev;
    }

    return elapsed;
}

function isMorayOverloaded(err) {
    //
    // This mess is how you detect if moray is overloaded.
    // When it is overloaded we'll want to back off a bit to try to prevent
    // making things worse.
    //
    cause = VError.findCauseByName(err, 'NoDatabasePeersError');
    if (cause !== null && cause.context && cause.context.name &&
        cause.context.message && cause.context.name === 'OverloadedError') {

        return true;
    }

    return false;
};

function ensureDelegated(callback) {
    forkExecWait({
        argv: [ '/usr/sbin/mount' ]
    }, function (err, info) {
        var found = false;
        var idx;
        var lines;
        var matchRe = new RegExp('^' + INSTRUCTION_ROOT + ' on zones\/[0-9a-f\-]+\/data ');

        if (err) {
            callback(err);
            return;
        }

        lines = info.stdout.trim().split('\n');
        for (idx = 0; idx < lines.length; idx++) {
            if (matchRe.test(lines[idx])) {
                console.error('match [' + lines[idx] + ']');
                found = true;
                break;
            } else {
                console.error('no match [' + lines[idx] + ']');
            }
        }

        callback(null, found);
    });
}

function writeGarbageInstructions(self, opts, callback) {
    assert.object(self, 'self');
    assert.object(self.config, 'self.config');
    assert.uuid(self.config.instance, 'self.config.instance');
    assert.object(self.log, 'self.log');
    assert.object(opts, 'opts');
    assert.object(opts.records, 'opts.records');
    assert.optionalUuid(opts.reqId, 'opts.reqId');
    assert.func(callback, 'callback');

    var beginning = process.hrtime();
    var filesWritten = 0;

    vasync.forEachPipeline({
        func: function _writeGarbageInstruction(storageId, cb) {
            var data = '';
            var date = new Date().toISOString().replace(/[-\:]/g, '').replace(/\..*$/, 'Z');
            var dirnameFinal;
            var dirnameTemp;
            var filename;
            var filenameFinal;
            var filenameTemp;
            var idx;
            var record;

            filename = [date, self.config.instance, 'X', uuid(), 'mako', storageId].join('-') + '.instruction';
            dirnameFinal = path.join(INSTRUCTION_ROOT, storageId);
            dirnameTemp = path.join(INSTRUCTION_ROOT, storageId + '.tmp');
            filenameFinal = path.join(dirnameFinal, filename);
            filenameTemp = path.join(dirnameTemp, filename);

            // XXX should we limit the number of instructions here, or is the
            // limit because of the gathering good enough?

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

            self.log.trace({
                data: data,
                dirnameTemp: dirnameTemp,
                dirnameFinal: dirnameFinal,
                records: opts.records[storageId],
                reqId: opts.reqId,
                storageId: storageId
            }, 'Writing Instructions.');

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
                self.log.debug({
                    err: err,
                    filename: filenameFinal,
                    reqId: opts.reqId
                }, 'Wrote file.');

                filesWritten++;
                cb(err);
            });
        },
        inputs: Object.keys(opts.records)
    }, function _pipelineComplete(err) {
        self.log.info({
            elapsed: elapsedSince(beginning),
            err: err,
            filesWritten: filesWritten,
            reqId: opts.reqId
        }, 'Wrote instruction files.');

        callback(null, {});
    });
};


module.exports = {
    CONFIG_FILE: CONFIG_FILE,
    FASTDELETE_BUCKET: FASTDELETE_BUCKET,
    INSTRUCTION_ROOT: INSTRUCTION_ROOT,
    createLogger: createLogger,
    elapsedSince: elapsedSince,
    ensureDelegated: ensureDelegated,
    getBucketsMdapiConfig: getBucketsMdapiConfig,
    getMorayConfig: getMorayConfig,
    isMorayOverloaded: isMorayOverloaded,
    loadConfig: loadConfig,
    validateConfig: validateConfig,
    writeGarbageInstructions: writeGarbageInstructions
};
