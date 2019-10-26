/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2019, Joyent, Inc.
 */

var fs = require('fs');

var assert = require('assert-plus');
var bunyan = require('bunyan');
var createMetricsManager = require('triton-metrics').createMetricsManager;
var jsprim = require('jsprim');
var verror = require('verror');
var VError = verror.VError;


var CONFIG_FILE = '/opt/smartdc/manta-garbage-collector/etc/config.json';
var FASTDELETE_BUCKET = 'manta_fastdelete_queue';
var INSTRUCTION_ROOT = '/var/spool/manta_gc';
var NS_PER_SEC = 1e9;

//
// Ideally we'd not bother using moray here since it really does us no favors,
// but there's currently no golden path to using manatee directly, so this is
// what we've got.
//
// TODO: do these all need to be indexes? ¯\_(ツ)_/¯
//
// I think we might only need an index on storageId and _id (which we get for
// free?) since we're only doing fetch based on those and deleteMany based on
// those.
//
var MANTA_GARBAGE_BUCKET = {
    index: {
        storageId: { type: 'string' }
    }
};
var MANTA_GARBAGE_BUCKET_NAME = 'manta_garbage';


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

    var parsed;

    if (!opts.config && !opts.filename) {
        opts.filename = CONFIG_FILE;
    }

    if (opts.filename) {
        opts.log.info(opts, 'loading config from file');

        fs.readFile(opts.filename, function _onReadFile(err, data) {
            if (!err) {
                //try {
                    parsed = JSON.parse(data.toString('utf8'));
                //} catch (e) {
                //}
            }
            callback(err, parsed);

            // done(new VE(err, 'loading file "%s"', ctx.ctx_cfgfile));
            return;

        });
        /*
        var out, uses_old_config = false;
        try {
            out = JSON.parse(data.toString('utf8'));
        } catch (e) {
            done(new VE(e, 'error parsing file "%s"',
                ctx.ctx_cfgfile));
            return;
        }

        ctx.ctx_log.info('loaded configuration file "%s"',
            ctx.ctx_cfgfile);
        */
    }
}

function validateConfig(opts, callback) {
    callback(null, {});
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


/*


function
setup_metrics(ctx, done)
{
    var metrics_manager = createMetricsManager({
        address: '0.0.0.0',
        log: ctx.ctx_log.child({
            component: 'MetricsManager'
        }),
        staticLabels: {
            datacenter: ctx.ctx_cfg.datacenter,
            instance: ctx.ctx_cfg.instance,
            server: ctx.ctx_cfg.server_uuid,
            service: ctx.ctx_cfg.service_name
        },
        port: ctx.ctx_cfg.port + 1000,
        restify: mod_restify
    });

    // Application layer metrics

    metrics_manager.collector.gauge({
        name: 'gc_cache_entries',
        help: 'total number of cache entries'
    });

    metrics_manager.collector.histogram({
        name: 'gc_delete_records_read',
        help: 'number of records read per rpc'
    });

    metrics_manager.collector.histogram({
        name: 'gc_delete_records_cleaned',
        help: 'number of records removed per rpc'
    });

    metrics_manager.collector.histogram({
        name: 'gc_mako_instrs_written',
        help: 'number of instructions written per local file'
    });

    metrics_manager.collector.histogram({
        name: 'gc_bytes_marked_for_delete',
        help: 'number of bytes marked for delete'
    });

    ctx.ctx_metrics_manager = metrics_manager;
    ctx.ctx_metrics_manager.listen(done);
}

*/


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


module.exports = {
    CONFIG_FILE: CONFIG_FILE,
    FASTDELETE_BUCKET: FASTDELETE_BUCKET,
    INSTRUCTION_ROOT: INSTRUCTION_ROOT,
    MANTA_GARBAGE_BUCKET: MANTA_GARBAGE_BUCKET,
    MANTA_GARBAGE_BUCKET_NAME: MANTA_GARBAGE_BUCKET_NAME,
    createLogger: createLogger,
    elapsedSince: elapsedSince,
    getMorayConfig: getMorayConfig,
    isMorayOverloaded: isMorayOverloaded,
    loadConfig: loadConfig,
    validateConfig: validateConfig
};
