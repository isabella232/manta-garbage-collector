/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */


/*
 * Status server, used for getting and tuning accelerated garbage collector
 * properties.
 */

var mod_assertplus = require('assert-plus');
var mod_jsprim = require('jsprim');
var mod_restify = require('restify');
var mod_util = require('util');
var mod_verror = require('verror');

var mod_schema = require('./schema');

var VE = mod_verror.VError;
var WE = mod_verror.WError;

/*
 * Maximum number of outstanding requests before serving 429s.
 */
var REQUEST_THRESHOLD = 10;


function
validate_shards(ctx, shards)
{
	var err = mod_schema.validate_shards_cfg(shards);
	if (err) {
		return (err);
	}

	for (var i = 0; i < shards.length; i++) {
		var shard = shards[i].host;
		if (!ctx.ctx_moray_clients.hasOwnProperty(shard)) {
			return (new Error('shard ' + shard + ' not ' +
			    'assigned to this collector'));
		}
	}

	return (null);
}


function
handle_ping(req, res, next)
{
	req.log.debug('ping');

	res.send(200, {
		ok: true,
		when: (new Date()).toISOString()
	});

	next();
}


function
handle_refresh_users(req, res, next)
{
	var ctx = req.ctx;

	req.log.debug('refresh users');

	ctx.ctx_gc_manager.refresh_users(function (err) {
		if (err) {
			res.send(500, {
				ok: false,
				err: err.message
			});
			return;
		}
		res.send(200, {
			ok: true,
			when: (new Date()).toISOString()
		});
	});
}


function
handle_get_workers(req, res, next)
{
	var ctx = req.ctx;

	res.send(200, ctx.ctx_gc_manager.get_workers());
}


function
handle_pause_workers(req, res, next)
{
	var ctx = req.ctx;
	var shards;
	var err;

	/*
	 * If no shards specified, pasue all workers for all shards.
	 */
	if (!req.body || !req.body.shards) {
		shards = Object.keys(ctx.ctx_moray_clients);
	} else {
		err = validate_shards(ctx, req.body.shards);
		if (err !== null) {
			res.send(400, {
				ok: false,
				err: err.message
			});
			return;
		}
		shards = req.body.shards.map(function (shard) {
			return (shard.host);
		});
	}

	ctx.ctx_gc_manager.pause_workers(shards, function () {
		res.send(200, {
			ok: true,
			when: (new Date()).toISOString()
		});
	});
}


function
handle_resume_workers(req, res, next)
{
	var ctx = req.ctx;
	var shards;
	var err;

	if (!req.body || !req.body.shards) {
		shards = Object.keys(ctx.ctx_moray_clients);
	} else {
		err = validate_shards(ctx, req.body.shards);
		if (err !== null) {
			res.send(400, {
				ok: false,
				err: err.message
			});
			return;
		}
		shards = req.body.shards.map(function (shard) {
			return (shard.host);
		});
	}

	ctx.ctx_gc_manager.resume_workers(shards, function () {
		res.send(200, {
			ok: true,
			when: (new Date()).toISOString()
		});
	});
}


function
handle_shards_cfg_get(req, res, next)
{
	var ctx = req.ctx;

	res.send(200, ctx.ctx_cfg.shards);
}


function
handle_buckets_cfg_get(req, res, next)
{
	var ctx = req.ctx;

	res.send(200, ctx.ctx_cfg.buckets);
}


function
handle_tunables_cfg_get(req, res, next)
{
	var ctx = req.ctx;

	res.send(200, ctx.ctx_cfg.tunables);
}


function
handle_tunables_cfg_post(req, res, next)
{
	var ctx = req.ctx;
	var tunables = req.body;

	var err = mod_schema.validate_tunables_update_cfg(tunables);
	if (err) {
		res.send(400, {
			ok: false,
			err: err.message
		});
		return;
	}

	var old_tunables = ctx.ctx_cfg.tunables;
	var new_tunables = mod_jsprim.mergeObjects(old_tunables,
		tunables);

	ctx.ctx_cfg.tunables = new_tunables;

	res.send(200, {
		ok: true,
		when: (new Date()).toISOString()
	});
}


function
create_http_server(ctx, done)
{
	var work_queue = [];
	var s = mod_restify.createServer({
		name: 'manta-garbage-collector',
		version: '1.0.0',
		handleUncaughtExceptions: true
	});
	s.use(mod_restify.plugins.bodyParser({
		rejectUnknown: false,
		mapParams: true
	}));
	s.use(mod_restify.plugins.requestLogger());
	s.use(function (req, res, next) {
		req.ctx = ctx;
		next();
	});
	s.use(function (req, res, next) {
		if (work_queue.length >= REQUEST_THRESHOLD) {
			res.send(429, {
				ok: false,
				when: (new Date()).toISOString(),
				queued: work_queue.length
			});
			return;
		}
		if (work_queue.length > 0) {
			work_queue.push(next);
			return;
		}
		setImmediate(next);
	});
	s.on('after', function (_) {
		var next = work_queue.pop();
		if (next) {
			setImmediate(next);
		}
	});

	s.get('/ping', handle_ping);

	s.get('/users/refresh', handle_refresh_users);

	s.get('/workers/get', handle_get_workers);
	s.post('/workers/pause', handle_pause_workers);
	s.post('/workers/resume', handle_resume_workers);

	s.get('/shards', handle_shards_cfg_get);

	s.get('/tunables', handle_tunables_cfg_get);
	s.post('/tunables', handle_tunables_cfg_post);

	s.get('/buckets', handle_buckets_cfg_get);

	s.listen(ctx.ctx_cfg.port, ctx.ctx_cfg.address, function (err) {
		if (err) {
			done(new VE(err, 'Restify listen on port %d.',
				ctx.ctx_cfg.port));
			return;
		}

		ctx.ctx_http_server = s;
		done();
	});
}

module.exports = {
	create_http_server: create_http_server
};
