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

var mod_restify = require('restify');
var mod_util = require('util');
var mod_verror = require('verror');

var VE = mod_verror.VError;


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

	ctx.ctx_gc_manager.pause_gc_workers(req.body.shards, function () {
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

	ctx.ctx_gc_manager.resume_gc_workers(req.body.shards, function () {
		res.send(200, {
			ok: true,
			when: (new Date()).toISOString()
		});
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
