/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */


var mod_bunyan = require('bunyan');
var mod_fs = require('fs');
var mod_jsprim = require('jsprim');
var mod_manta = require('manta');
var mod_moray = require('moray');
var mod_path = require('path');
var mod_vasync = require('vasync');
var mod_verror = require('verror');

var mod_gc_manager = require('../lib/gc_manager');
var mod_schema = require('../lib/schema');

var lib_common = require('../lib/common');
var lib_http_server = require('../lib/http_server');

var VE = mod_verror.VError;


function
retry(func, ctx, done, nsecs)
{
	setTimeout(function () {
		func(ctx, done);
	}, nsecs * 1000);
}

function
load_config(ctx, done)
{
	ctx.ctx_log.debug('loading configuration file "%s"', ctx.ctx_cfgfile);
	mod_fs.readFile(ctx.ctx_cfgfile, function (err, data) {
		if (err) {
			if (err.code === 'ENOENT') {
				if (ctx.ctx_cfgfile_notfound++ === 0) {
					ctx.ctx_log.info('waiting for ' +
						'config file "%s"',
						ctx.ctx_cfgfile);
				}
				retry(load_config, ctx, done, 1);
				return;
			}

			done(new VE(err, 'loading file "%s"', ctx.ctx_cfgfile));
			return;

		}
		var out;
		try {
			out = JSON.parse(data.toString('utf8'));
		} catch (e) {
			done(new VE(e, 'error parsing file "%s"', ctx.ctx_cfgfile));
			return;
		}

		ctx.ctx_log.info('loaded configuration file "%s"', ctx.ctx_cfgfile);

		var err = mod_schema.validate_shards_cfg(out.shards);
		if (err) {
			done(new VE(err, 'malformed shards ocnfiguration'));
			return;
		}

		var err = mod_schema.validate_moray_cfg(out.params.moray);
		if (err) {
			done(new VE(err, 'malformed moray configuration'));
			return;
		}

		err = mod_schema.validate_mako_cfg(out.params.mako);
		if (err) {
			done(new VE(err, 'malformed mako configuration'));
			return;
		}

		ctx.ctx_cfg = out;

		setImmediate(done);
	});
}

function
setup_moray_clients(ctx, done)
{
	ctx.ctx_moray_clients = {};
	ctx.ctx_moray_cfgs = {};

	var moray_defaults = {
		record_read_offset: 0
	};

	mod_vasync.forEachPipeline({
		inputs: ctx.ctx_cfg.shards,
		func: function create_moray_client(shard, next) {
			lib_common.create_moray_client(ctx, shard.host,
				function (err, client) {
				if (err) {
					next(err);
					return;
				}
				ctx.ctx_moray_clients[shard.host] = client;
				ctx.ctx_moray_cfgs[shard.host] = mod_jsprim.mergeObjects(
					shard, ctx.ctx_cfg.params.moray, moray_defaults);
				next();
			});

		}
	}, function (err) {
		if (err) {
			done(new VE(err, 'creating moray clients'));
			return;
		}
		done();
	});
}

function
setup_manta_client(ctx, done)
{
	var overrides = {
		log: ctx.ctx_log
	};
	var manta_cfg = mod_jsprim.mergeObjects(ctx.ctx_cfg.manta, overrides, null);

	ctx.ctx_manta_client = mod_manta.createClient(manta_cfg);
	ctx.ctx_log.debug('created manta client');

	ctx.ctx_mako_cfg = mod_jsprim.deepCopy(ctx.ctx_cfg.params.mako);
	setImmediate(done);
}


(function
main()
{
	var ctx = {
		ctx_cfgfile: mod_path.join(__dirname, '..', 'etc',
			'config.json'),
		ctx_cfgfile_notfound: 0
	};

	var log = ctx.ctx_log = mod_bunyan.createLogger({
		name: 'garbage-collector',
	    	level: process.env.LOG_LEVEL || mod_bunyan.DEBUG,
	    	serializers: mod_bunyan.stdSerializers
	});

	mod_vasync.pipeline({ arg: ctx, funcs: [
		/*
		 * Load the configuration file. This contains all requisite
		 * information for creating the clients below.
		 */
		load_config,

		/*
		 * Create one node-moray client per shard specified by the
		 * configuration. Options passed to the client are uniform and
		 * specified in the file loaded in load_config.
		 */
		setup_moray_clients,

		/*
		 * Create one manta client.
		 */
		setup_manta_client,

		/*
		 * Create the restify server used for exposing configuration to
		 * the operator.
		 */
		lib_http_server.create_http_server,

		/*
		 * Create the gc manager
		 */
		mod_gc_manager.create_gc_manager
	] }, function (err) {
		if (err) {
			log.fatal(err, 'startup failure');
			process.exit(1);
		}

		log.info('startup complete');
	});
})();
