/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2019, Joyent, Inc.
 */

/*
 * Common utility functions.
 */

var path = require('path');

var assert = require('assert-plus');
var ajv = require('ajv');

/*
{
        "moray": {
                "options": {
                        "cueballOptions": {
                                "resolvers": [
                                        "nameservice.eu-central.scloud.host"
                                ]
                        }
                }
        },
        "shards": [
                {
                        "host": "2.moray.eu-central.scloud.host"
                },
                {
                        "host": "3.moray.eu-central.scloud.host"
                },
                {
                        "host": "4.moray.eu-central.scloud.host"
                },
                {
                        "host": "5.moray.eu-central.scloud.host"
                },
                {
                        "host": "6.moray.eu-central.scloud.host"
                },
                {
                        "host": "7.moray.eu-central.scloud.host"
                }
        ],
        "options": {
                "record_read_batch_size": 3500,
                "record_delete_delay": 500
        },
}
*/

var CONFIG_SCHEMA = {
    properties: {
        datacenter: {
            minLength: 1,
            type: 'string'
        },
        domain: {
            minLength: 1,
            type: 'string'
        },
        instance: {
            minLength: 1,
            type: 'string'
        },
        sapiUrl: {
            minLength: 1,
            type: 'string'
        },
        serverUuid: {
            minLength: 1,
            type: 'string'
        },
        serviceName: {
            minLength: 1,
            type: 'string'
        },
        shards: {
            items: {
                properties: {
                    host: {
                        type: 'string'
                    }
                },
                required: ['host'],
                type: 'object'
            },
            minItems: 1,
            type: 'array',
            uniqueItems: true,
        },
        shardServiceName: {
            minLength: 1,
            type: 'string'
        },
        options: {
            recordReadSize: {
                type: 'integer',
                minimum: 1
            },
            recordDeleteDelay: { // XXX Do we *really* need this or just backoff?
                type: 'integer',
                minimum: 0
            }
        }
    }
};
var DEFAULT_CONFIG_FILE = path.join(__dirname, '../etc/config.json');


function loadConfig(configFile, callback) {
    assert.optionalString(configFile, 'configFile');
    assert.func(callback, 'callback');

    var filename = configFile || DEFAULT_CONFIG_FILE;

    fs.readFile(filename, function _onRead(err, data) {
        callback(err, data ? data.toString() : undefined);
    });
}

function validateConfig(config) {
    assert.object(config, 'config');

    var valid = ajv.validate(CONFIG_SCHEMA, config);
    if (!valid) {
        // XXX don't throw
        throw new Error(ajv.errors);
    }
}


/*
 * Create moray client and add it to the callers context object.
 */
function
create_moray_client(ctx, shard, done)
{
    var moray_options = ctx.ctx_cfg.moray.options;

    var metrics_manager = ctx.ctx_metrics_manager;
    var collector = (metrics_manager) ? metrics_manager.collector :
        undefined;

    var moray_cfg = function (domain) {
        var overrides = {
            collector: collector,
            srvDomain: domain,
            log: ctx.ctx_log.child({ shard: domain })
        };
        return (mod_jsprim.mergeObjects(moray_options,
            overrides, null));
    };

    var client = mod_moray.createClient(moray_cfg(shard));

    var timer = setTimeout(function () {
        client.removeAllListeners('connect');
        client.removeAllListeners('error');

        client.close();

        done(new VE('timed out trying to connect moray client: "%s"',
            shard));
    }, MORAY_CONNECT_TIMEOUT);

    client.once('connect', function () {
        clearTimeout(timer);
        ctx.ctx_log.debug('connected moray client: "%s"', shard);

        client.removeAllListeners('error');

        mod_assertplus.ok(ctx.ctx_moray_clients[shard] === undefined,
            'unexpected moray client');

        done(null, client);
    });

    client.once('error', function (err) {
        clearTimeout(timer);
        ctx.ctx_log.error({
            shard: shard,
            err: err
        }, 'error while connecting moray client');
        client.removeAllListeners('connect');

        done(err);
    });
}

module.exports = {
    loadConfig: loadConfig,
    validateConfig: validateConfig // exported for testing
};
