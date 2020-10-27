/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

var fs = require('fs');
var net = require('net');
var path = require('path');
var util = require('util');

var assert = require('assert-plus');
var bunyan = require('bunyan');
var forkExecWait = require('forkexec').forkExecWait;
var jsprim = require('jsprim');
var restifyClients = require('restify-clients');
var uuid = require('uuid/v4');
var vasync = require('vasync');
var verror = require('verror');
var VError = verror.VError;

var CONFIG_FILE = '/opt/smartdc/manta-garbage-collector/etc/config.json';
var FASTDELETE_BUCKET = 'manta_fastdelete_queue';
var INSTRUCTION_ROOT = '/var/spool/manta_gc';
var MANTA_BUCKET = 'manta';
var MAX_MPU_PARTS = 800;
var MPU_FINALIZE_BUCKET = 'manta_uploads';
var NS_PER_SEC = 1e9;

function createLogger(opts) {
    assert.object(opts, 'opts');
    assert.string(opts.name, 'opts.name');

    var logger = bunyan.createLogger({
        level: opts.level || process.env.LOG_LEVEL || bunyan.INFO,
        name: opts.name,
        serializers: restifyClients.bunyan.serializers
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

//
// This does basically the same thing as vasync.forEachParallel, but allows for
// a 'concurrency' parameter to limit how many are being done at once.
//
// Note: This is also used in manta-mako/lib/garbage-deleter.js and should
//       someday be moved somewhere it can be shared without duplication.
//
// When using 'concurrency' we assume the goal is to handle lots of items so
// results.success is a counter and results.operations is not used.
//
// You can also set a 'maxQueued' parameter which limits how many inputs are
// queued at once. This is useful if the total number of inputs is very large
// and you don't want vasync.queue to run itself out of memory storing all its
// temporary state. Every 'queueDelay' milliseconds it will attempt to bring the
// number queued back up to maxQueued until all inputs have been queued.
//
function forEachParallel(opts, callback) {
    assert.object(opts, 'opts');
    assert.optionalNumber(opts.concurrency, 'opts.concurrency');
    assert.func(opts.func, 'opts.func');
    assert.array(opts.inputs, 'opts.inputs');
    assert.func(callback, 'callback');
    assert.optionalNumber(opts.maxQueued, 'opts.maxQueued');
    assert.optionalNumber(opts.queueDelay, 'opts.queueDelay');

    var concurrency = opts.concurrency ? Math.floor(opts.concurrency) : 0;
    var error = null;
    var idx;
    var maxQueued = opts.maxQueued ? Math.floor(opts.maxQueued) : concurrency;
    var queue;
    var queueDelay = opts.queueDelay ? Math.floor(opts.queueDelay) : 1;
    var results = {
        ndone: 0,
        nerrors: 0,
        successes: 0
    };

    if (!concurrency) {
        // If they didn't want concurrency control, just give them the original
        // vasync.forEachParallel.
        vasync.forEachParallel(opts, callback);
        return;
    }

    // Tell node.js to use up to "concurrency" threads for IO so that our
    // blocking stuff actually does happen in parallel. (Node's default is 4)
    if (concurrency > process.env.UV_THREADPOOL_SIZE) {
        process.env.UV_THREADPOOL_SIZE = concurrency;
    }

    queue = vasync.queue(opts.func, concurrency);

    queue.on('end', function() {
        callback(error, results);
    });

    function _doneOne(err, result) {
        var _status;

        results.ndone++;
        if (err) {
            results.nerrors++;
            _status = 'fail';
            // Yes this overwrites with the last error seen.
            error = err;
        } else {
            results.successes++;
            _status = 'ok';
        }
    }

    function _queueItems() {
        if (idx < opts.inputs.length) {
            while (
                idx < opts.inputs.length &&
                queue.queued.length < maxQueued
            ) {
                queue.push(opts.inputs[idx], _doneOne);
                idx++;
            }

            setTimeout(_queueItems, queueDelay);
        } else {
            queue.close();
        }
    }

    idx = 0;
    _queueItems();
}

function validateConfig(config, callback) {
    assert.object(config, 'config');

    var idx;

    try {
        assert.string(config.admin_ip, 'config.admin_ip');
        assert.ok(
            net.isIPv4(config.admin_ip),
            'config.admin_ip is an IPv4 addr'
        );

        assert.optionalObject(config.buckets_mdapi, 'config.buckets_mdapi');
        assert.optionalArrayOfObject(
            config.buckets_shards,
            'config.buckets_shards'
        );
        if (config.buckets_shards) {
            for (idx = 0; idx < config.buckets_shards.length; idx++) {
                assert.string(
                    config.buckets_shards[idx].host,
                    'config.buckets_shards[' + idx + '].host'
                );
            }
        }

        assert.string(config.datacenter, 'config.datacenter');
        assert.ok(
            config.datacenter.match(/^[a-z0-9-]+$/),
            'config.datacenter must be a valid lower-case DNS name'
        );

        assert.optionalArrayOfObject(config.dir_shards, 'config.dir_shards');
        if (config.dir_shards) {
            for (idx = 0; idx < config.dir_shards.length; idx++) {
                assert.string(
                    config.dir_shards[idx].host,
                    'config.dir_shards[' + idx + '].host'
                );
            }
        }
        assert.optionalObject(config.moray, 'config.moray');

        assert.optionalNumber(config.dir_batch_size, 'config.dir_batch_size');
        assert.optionalNumber(
            config.dir_batch_interval_ms,
            'config.dir_batch_interval_ms'
        );
        assert.optionalNumber(
            config.dir_slow_put_cutoff,
            'config.dir_slow_put_cutoff'
        );
        assert.optionalNumber(
            config.dir_slow_put_max_failures,
            'config.dir_slow_put_max_failures'
        );
        assert.optionalNumber(
            config.dir_slow_put_disable_time,
            'config.dir_slow_put_disable_time'
        );

        assert.optionalNumber(
            config.buckets_batch_interval_ms,
            'config.buckets_batch_interval_ms'
        );

        assert.uuid(config.instance, 'config.instance');
        assert.uuid(config.server_uuid, 'config.server_uuid');
    } catch (e) {
        callback(e);
        return;
    }

    callback();
}

function getBucketsMdapiConfig(opts) {
    assert.object(opts, 'opts');
    assert.optionalObject(opts.collector, 'opts.collector');
    assert.object(opts.config, 'opts.config');
    assert.object(opts.config.buckets_mdapi, 'opts.config.buckets_mdapi');
    assert.object(
        opts.config.buckets_mdapi.options,
        'opts.config.buckets_mdapi.options'
    );
    assert.optionalObject(
        opts.config.buckets_mdapi.options.cueballOptions,
        'opts.config.buckets_mdapi.options.cueballOptions'
    );
    assert.object(opts.log, 'opts.log');
    assert.string(opts.bucketsMdapiShard, 'opts.bucketsMdapiShard');

    // Set some default cueball options since jsprim doesn't handle nested objects
    var cueballOptions = jsprim.mergeObjects(
        opts.config.buckets_mdapi.options.cueballOptions,
        {},
        {
            maximum: 1,
            spares: 1,
            target: 1
        }
    );

    // We merge in the default options and override with the cueball options we
    // already worked out above.
    var bucketsMdapiConfig = jsprim.mergeObjects(
        opts.config.buckets_mdapi.options,
        {cueballOptions: cueballOptions},
        {
            collector: opts.collector,
            crc_mode: 3,
            log: opts.log,
            srvDomain: opts.bucketsMdapiShard
        }
    );

    return bucketsMdapiConfig;
}

function getEMorayConfig(opts) {
    // Copy the options so we don't have side-effects for the caller.
    var optsCopy = jsprim.deepCopy(opts);

    optsCopy.morayShard = opts.morayShard || 'electric-moray';
    optsCopy.config.moray = optsCopy.config.emoray;
    optsCopy.clientType = 'electric-moray';
    delete optsCopy.config.emoray;

    opts.log.trace(
        {
            optsCopy_config: optsCopy.config,
            optsCopy_moray_shard: optsCopy.morayShard,
            opts_config: opts.config,
            opts_moray_shard: opts.morayShard
        },
        'getEMorayConfig'
    );

    return getMorayConfig(optsCopy);
}

function getMorayConfig(opts) {
    assert.object(opts, 'opts');
    assert.optionalObject(opts.collector, 'opts.collector');
    assert.object(opts.config, 'opts.config');
    assert.object(opts.config.moray, 'opts.config.moray');
    assert.object(opts.config.moray.options, 'opts.config.moray.options');
    assert.optionalObject(
        opts.config.moray.options.cueballOptions,
        'opts.config.moray.options.cueballOptions'
    );
    assert.object(opts.log, 'opts.log');
    assert.string(opts.morayShard, 'opts.morayShard');

    // Set some default cueball options since jsprim doesn't handle nested objects
    var cueballOptions = jsprim.mergeObjects(
        opts.config.moray.options.cueballOptions,
        {},
        {
            maximum: 1,
            spares: 1,
            target: 1
        }
    );
    var morayConfig;

    // We merge in the default options and override with the cueball options we
    // already worked out above.
    morayConfig = jsprim.mergeObjects(
        opts.config.moray.options,
        {cueballOptions: cueballOptions},
        {
            collector: opts.collector,
            log: opts.log,
            metricLabels: {
                clientType: opts.clientType || 'moray'
            },
            srvDomain: opts.morayShard
        }
    );

    return morayConfig;
}

function elapsedSince(beginning, prev) {
    var elapsed;
    var timeDelta;

    timeDelta = process.hrtime(beginning);
    elapsed = timeDelta[0] + timeDelta[1] / NS_PER_SEC;

    if (prev) {
        elapsed -= prev;
    }

    return elapsed;
}

function isMorayOverloaded(err) {
    var cause;

    //
    // This mess is how you detect if moray is overloaded.
    // When it is overloaded we'll want to back off a bit to try to prevent
    // making things worse.
    //
    cause = VError.findCauseByName(err, 'NoDatabasePeersError');
    if (
        cause !== null &&
        cause.context &&
        cause.context.name &&
        cause.context.message &&
        cause.context.name === 'OverloadedError'
    ) {
        return true;
    }

    return false;
}

function ensureDelegated(callback) {
    forkExecWait(
        {
            argv: ['/usr/sbin/mount']
        },
        function(err, info) {
            var matchRe = new RegExp(
                '^' + INSTRUCTION_ROOT + ' on zones/[0-9a-f-]+/data ',
                'm'
            );

            if (err) {
                callback(err);
                return;
            }

            callback(null, matchRe.test(info.stdout));
        }
    );
}

function writeGarbageInstructions(gcConsumer, opts, callback) {
    assert.object(gcConsumer, 'gcConsumer');
    assert.object(gcConsumer.config, 'gcConsumer.config');
    assert.uuid(gcConsumer.config.instance, 'gcConsumer.config.instance');
    assert.object(gcConsumer.log, 'gcConsumer.log');
    assert.object(opts, 'opts');
    assert.optionalString(opts.instructionRoot, 'opts.instructionRoot');
    assert.object(opts.records, 'opts.records');
    assert.optionalUuid(opts.reqId, 'opts.reqId');
    assert.optionalString(opts.type, 'opts.type');
    assert.func(callback, 'callback');

    if (opts.type) {
        assert.ok(['bucketsv2'].indexOf(opts.type) !== -1, 'opts.type');
    }

    var beginning = process.hrtime();
    var filesWritten = 0;
    var instrRoot = opts.instructionRoot || INSTRUCTION_ROOT;
    var instrsWritten = 0;

    vasync.forEachPipeline(
        {
            func: function _writeGarbageInstruction(storageId, cb) {
                var data = '';
                var date = new Date()
                    .toISOString()
                    .replace(/[-:]/g, '')
                    .replace(/\..*$/, 'Z');
                var dirnameFinal;
                var dirnameTemp;
                var filename;
                var filenameFinal;
                var filenameTemp;
                var idx;
                var record;

                filename =
                    [
                        date,
                        gcConsumer.config.instance,
                        'X',
                        uuid(),
                        'mako',
                        storageId
                    ].join('-') + '.instruction';
                dirnameFinal = path.join(instrRoot, storageId);
                dirnameTemp = path.join(instrRoot, storageId + '.tmp');
                filenameFinal = path.join(dirnameFinal, filename);
                filenameTemp = path.join(dirnameTemp, filename);

                // NOTE: We currently do not limit the number of instructions
                //       we're writing to each file because we rely on the limit
                //       at the point of *gathering* the records which is done
                //       in batches. I.e. we'll get sent an opts.records which
                //       has a limited set of records.

                // Build the actual "instructions" content from the records
                for (idx = 0; idx < opts.records[storageId].length; idx++) {
                    record = opts.records[storageId][idx];

                    assert.number(record.bytes, 'record.bytes');
                    assert.uuid(record.objectId, 'record.objectId');
                    assert.uuid(record.ownerId, 'record.ownerId');
                    assert.string(record.shard, 'record.shard');
                    assert.string(record.storageId, 'record.storageId');

                    if (opts.type === 'bucketsv2') {
                        assert.uuid(record.bucketId, 'record.bucketId');
                        assert.string(record.hash, 'record.hash');

                        data += util.format(
                            '%s\tDELETEv2\t%s\t%s\t%d\n',
                            record.storageId,
                            path.join(
                                '/v2',
                                record.ownerId,
                                record.bucketId,
                                record.objectId.substr(0, 2),
                                record.objectId + ',' + record.hash
                            ),
                            record.shard,
                            record.bytes
                        );
                    } else {
                        data += util.format(
                            '%s\t%s\t%s\t%s\t%d\n',
                            record.storageId,
                            record.ownerId,
                            record.objectId,
                            record.shard,
                            record.bytes
                        );
                    }
                }

                gcConsumer.log.trace(
                    {
                        data: data,
                        dirnameTemp: dirnameTemp,
                        dirnameFinal: dirnameFinal,
                        records: opts.records[storageId],
                        reqId: opts.reqId,
                        storageId: storageId
                    },
                    'Writing Instructions.'
                );

                vasync.pipeline(
                    {
                        funcs: [
                            function _mkdirTemp(_, next) {
                                fs.mkdir(dirnameTemp, function _onMkdir(err) {
                                    if (err && err.code !== 'EEXIST') {
                                        next(err);
                                        return;
                                    }
                                    next();
                                });
                            },
                            function _writeFileTemp(_, next) {
                                fs.writeFile(filenameTemp, data, next);
                            },
                            function _mkdirFinal(_, next) {
                                fs.mkdir(dirnameFinal, function _onMkdir(err) {
                                    if (err && err.code !== 'EEXIST') {
                                        next(err);
                                        return;
                                    }
                                    next();
                                });
                            },
                            function _mvFileFinal(_, next) {
                                fs.rename(filenameTemp, filenameFinal, next);
                            }
                        ]
                    },
                    function _writeFilePipeline(err) {
                        if (!err) {
                            instrsWritten += opts.records[storageId].length;
                            filesWritten++;
                        }

                        gcConsumer.log.debug(
                            {
                                err: err,
                                filename: filenameFinal,
                                reqId: opts.reqId
                            },
                            'Write file.'
                        );

                        cb(err);
                    }
                );
            },
            inputs: Object.keys(opts.records)
        },
        function _pipelineComplete(err) {
            gcConsumer.log[err ? 'error' : 'info'](
                {
                    elapsed: elapsedSince(beginning),
                    err: err,
                    filesWritten: filesWritten,
                    instrsWritten: instrsWritten,
                    reqId: opts.reqId
                },
                'Finished writing instruction files.'
            );

            callback(err, {
                filesWritten: filesWritten,
                instrsWritten: instrsWritten
            });
        }
    );
}

function addCounter(counterName, value, labels) {
    var self = this;

    // For tests, we don't want to require a full metricManager, so in that case
    // we just manually manage the values in the "metrics" object.
    if (!self.metricsManager) {
        if (!self.metrics.hasOwnProperty(counterName)) {
            self.metrics[counterName] = 0;
        }
        self.metrics[counterName] += value;
        return;
    }

    self.metrics[counterName].add(value, labels);
}

function getCounter(counterName) {
    var self = this;

    if (!self.metricsManager) {
        return self.metrics[counterName];
    }

    return self.metrics[counterName].getValue();
}

function getGauge(gaugeName) {
    var self = this;

    if (!self.metricsManager) {
        return self.metrics[gaugeName];
    }

    return self.metrics[gaugeName].getValue();
}

function setGauge(gaugeName, value, labels) {
    var self = this;

    // For tests, we don't want to require a full metricManager, so in that case
    // we just manually manage the values in the "metrics" object.
    if (!self.metricsManager) {
        self.metrics[gaugeName] = value;
        return;
    }

    self.metrics[gaugeName].set(value, labels);
}

module.exports = {
    CONFIG_FILE: CONFIG_FILE,
    FASTDELETE_BUCKET: FASTDELETE_BUCKET,
    INSTRUCTION_ROOT: INSTRUCTION_ROOT,
    MANTA_BUCKET: MANTA_BUCKET,
    MAX_MPU_PARTS: MAX_MPU_PARTS,
    MPU_FINALIZE_BUCKET: MPU_FINALIZE_BUCKET,
    addCounter: addCounter,
    createLogger: createLogger,
    elapsedSince: elapsedSince,
    ensureDelegated: ensureDelegated,
    forEachParallel: forEachParallel,
    getBucketsMdapiConfig: getBucketsMdapiConfig,
    getCounter: getCounter,
    getEMorayConfig: getEMorayConfig,
    getGauge: getGauge,
    getMorayConfig: getMorayConfig,
    isMorayOverloaded: isMorayOverloaded,
    loadConfig: loadConfig,
    setGauge: setGauge,
    validateConfig: validateConfig,
    writeGarbageInstructions: writeGarbageInstructions
};
