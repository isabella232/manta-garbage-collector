/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

/*
 * Common utility functions.
 */

var mod_assertplus = require('assert-plus');
var mod_jsprim = require('jsprim');
var mod_moray = require('moray');
var mod_verror = require('verror');


var MORAY_CONNECT_TIMEOUT = 10000;

var VE = mod_verror.VError;


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
	create_moray_client: create_moray_client
};
