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
handle_get_workers(req, res, next)
{
	var ctx = req.ctx;

	res.send(200, ctx.ctx_gc_manager.get_gc_workers());
}


function
handle_pause_workers(req, res, next)
{
	var ctx = req.ctx;

	var shards;

	/*
	 * If no shards specified, pasue all workers for all shards.
	 */
	if (!req.body || !req.body.shards) {
		shards = Object.keys(ctx.ctx_moray_cfgs);
	} else {
		shards = req.body.shards;
	}

	ctx.ctx_gc_manager.pause_gc_workers(shards, function () {
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

	if (!req.body || !req.body.shards) {
		shards = Object.keys(ctx.ctx_moray_cfgs);
	} else {
		shards = req.body.shards;
	}

	ctx.ctx_gc_manager.resume_gc_workers(shards, function () {
		res.send(200, {
			ok: true,
			when: (new Date()).toISOString()
		});
	});
}


function
handle_shard_cfg_get(req, res, next)
{
	var ctx = req.ctx;
	var shard = req.params.shard;

	if (!ctx.ctx_moray_cfgs.hasOwnProperty(shard)) {
		setImmediate(next, new WE('no gc workers for shard "%s"',
			shard));
		return;
	}

	mod_assertplus.object(ctx.ctx_moray_cfgs[shard],
		'missing shard configuration');

	res.send(200, ctx.ctx_moray_cfgs[shard]);
};


function
handle_shard_cfg_post(req, res, next)
{
	var ctx = req.ctx;
	var shard = req.params.shard;
	var updates = req.body;

	mod_assertplus.object(updates, 'updates');

	var err = mod_schema.validate_update_moray_cfg(updates);
	if (err !== null) {
		res.send(400, {
			ok: false,
			err: err.message
		});
		return;
	}

	if (!ctx.ctx_moray_cfgs.hasOwnProperty(shard)) {
		setImmediate(next, new WE('no gc workers for shard "%s"',
			shard));
		return;
	}

	var curr = ctx.ctx_moray_cfgs[shard];
	mod_assertplus.object(curr, 'curr');

	ctx.ctx_moray_cfgs[shard] = mod_jsprim.mergeObjects(curr, updates);

	function finish_update(err) {
		if (err) {
			res.send(500, {
				ok: false,
				when: (new Date()).toISOString()
			});
			return;
		}
		res.send(200, {
			ok: true,
			when: (new Date()).toISOString()
		});
	}

	/*
	 * If the update contains a 'buckets' field, then we are potentially
	 * changing the gc worker count. Once the configuration in
	 * 'ctx_moray_cfgs' is updated, we ping to gc manager to pick up the
	 * change and create/remove workers as necessary.
	 */
	var is_bucket_update = updates.buckets !== undefined;
	if (is_bucket_update) {
		ctx.ctx_gc_manager.ensure_gc_workers(finish_update);
	} else {
		setImmediate(finish_update);
	}
};


function
handle_mako_cfg_get(req, res, next)
{
	var ctx = req.ctx;

	res.send(200, ctx.ctx_mako_cfg);
}


function
handle_mako_cfg_post(req, res, next)
{
	var ctx = req.ctx;
	var curr = ctx.ctx_mako_cfg;
	var updates = req.body;

	mod_assertplus.object(curr, 'curr');
	mod_assertplus.object(updates, 'updates');

	var err = mod_schema.validate_update_mako_cfg(updates);
	if (err !== null) {
		res.send(400, {
			ok: false,
			err: err.message
		});
		return;
	}

	ctx.ctx_mako_cfg = mod_jsprim.mergeObjects(curr, updates);

	res.send(200, {
		ok: true,
		when: (new Date()).toISOString()
	});
}


function
create_http_server(ctx, done)
{
	var port = 80;

	var s = mod_restify.createServer({
		name: 'manta-garbage-collector',
	    	version: '1.0.0',
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

	s.get('/ping', handle_ping);

	s.get('/workers/get', handle_get_workers);
	s.post('/workers/pause', handle_pause_workers);
	s.post('/workers/resume', handle_resume_workers);

	s.get('/mako', handle_mako_cfg_get);
	s.post('/mako', handle_mako_cfg_post);

	s.get('/shards/:shard', handle_shard_cfg_get);
	s.post('/shards/:shard', handle_shard_cfg_post);


	s.listen(port, function (err) {
		if (err) {
			done(new VE(err, 'restify listen on port %d', port));
			return;
		}

		ctx.ctx_http_server = s;
		done();
	});
}

module.exports = {
	create_http_server: create_http_server
};
