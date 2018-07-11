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

/*
 * Create moray client and add it to the callers context object.
 */
function
create_moray_client(ctx, shard, done)
{
	var cfg = ctx.ctx_cfg;
	var moray_options = ctx.ctx_cfg.moray.options;

	var moray_cfg = function (shard) {
		var overrides = {
			srvDomain: shard,
			log: ctx.ctx_log.child({ shard: shard })
		};
		return mod_jsprim.mergeObjects(moray_options, overrides, null);
	};

	var client = mod_moray.createClient(moray_cfg(shard));

	client.once('connect', function () {
		ctx.ctx_log.debug('connected moray client: "%s"', shard);

		client.removeAllListeners('error');

		mod_assertplus.ok(ctx.ctx_moray_clients[shard] === undefined,
			'unexpected moray configuration while creating client');
		done(null, client);
	});

	client.once('error', function (err) {
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
