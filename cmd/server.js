/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2019, Joyent, Inc.
 */


var mod_assertplus = require('assert-plus');
var mod_bunyan = require('bunyan');
var mod_fs = require('fs');
var mod_jsprim = require('jsprim');
var mod_manta = require('manta');
var mod_moray = require('moray');
var mod_path = require('path');
var mod_restify = require('restify');
var mod_sdc = require('sdc-clients');
var mod_util = require('util');
var mod_vasync = require('vasync');
var mod_verror = require('verror');

var mod_gc_manager = require('../lib/gc_manager');
var mod_schema = require('../lib/schema');

var lib_common = require('../lib/common');
var lib_http_server = require('../lib/http_server');
var lib_triton_access = require('../lib/triton_access');

var createMetricsManager = require('triton-metrics').createMetricsManager;
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

		var schema_err = mod_schema.validate_creators_cfg(
			out.allowed_creators || out.creators);
		if (schema_err) {
			done(new VE(schema_err, 'malformed creators config'));
			return;
		}

		schema_err = mod_schema.validate_shards_cfg(
			out.shards);
		if (schema_err) {
			schema_err = mod_schema.validate_shards_old_cfg(
			    out.shards);
			if (schema_err) {
				done(new VE(schema_err, 'malformed shards ' +
					'configuration'));
				return;
			}
			uses_old_config = true;
			ctx.ctx_log.warn('assuming old configuration schema');
		}

		schema_err = mod_schema.validate_buckets_cfg(
			out.buckets);
		if (schema_err) {
			done(new VE(schema_err, 'malformed buckets ' +
			    'configuration'));
			return;
		}

		schema_err = mod_schema.validate_tunables_cfg(
			out.tunables);
		if (schema_err) {
			done(new VE(schema_err, 'malformed tunables ' +
			    'configuration'));
			return;
		}

		ctx.ctx_old_config = uses_old_config;
		ctx.ctx_cfg = out;

		setImmediate(done);
	});
}


function
translate_config(ctx, done)
{
	if (!ctx.ctx_old_config) {
		done();
		return;
	}
	var shards = ctx.ctx_cfg.shards;
	var assigned_shards = [];

	if (!(shards[0] === 0 && shards[1] === 0)) {
		for (var i = shards[0]; i <= shards[1]; i++) {
			assigned_shards.push({
			    host: [i, ctx.ctx_cfg.shard_service_name || 'moray',
				ctx.ctx_cfg.domain].join('.')
			});
		}
	}

	ctx.ctx_cfg.shards = assigned_shards;
	done();
}


function
setup_log(ctx, done)
{
	var bunyan_cfg = ctx.ctx_cfg.bunyan;
	mod_assertplus.object(bunyan_cfg, 'bunyan_cfg');

	if (bunyan_cfg.level) {
		mod_assertplus.string(bunyan_cfg.level, 'bunyan_cfg.level');
		ctx.ctx_log.level(bunyan_cfg.level);
	}

	done();
}


function
set_global_ctx_fields(ctx, done)
{
	mod_assertplus.object(ctx.ctx_cfg, 'ctx.ctx_cfg');

	ctx.ctx_total_cache_entries = 0;

	done();
}


function
setup_sapi_client(ctx, done)
{
	ctx.ctx_sapi = new mod_sdc.SAPI({
		url: ctx.ctx_cfg.sapi_url,
		log: ctx.ctx_log.child({
			component: 'SAPI'
		}),
		version: '*',
		agent: false
	});

	ctx.ctx_log.debug('Created SAPI client.');

	done();
}


function
load_manta_application(ctx, done)
{
	lib_triton_access.get_sapi_application(ctx.ctx_log, ctx.ctx_sapi,
		'manta', true, function (err, app) {
		if (err) {
			done(err);
		}

		ctx.ctx_manta_app = app;
		ctx.ctx_log.debug('Loaded manta application.');

		done();
	});
}


function
setup_manta_client(ctx, done)
{
	var overrides = {
		log: ctx.ctx_log
	};
	var manta_cfg = mod_jsprim.mergeObjects(ctx.ctx_cfg.manta,
		overrides, null);

	ctx.ctx_manta_client = mod_manta.createClient(manta_cfg);
	ctx.ctx_log.debug('created manta client');

	setImmediate(done);
}


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

	/*
	 * Application layer metrics
	 */

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
		name: 'gc_mako_instrs_uploaded',
		help: 'number of instructions uploaded per manta put'
	});

	metrics_manager.collector.histogram({
		name: 'gc_bytes_marked_for_delete',
		help: 'number of bytes marked for delete'
	});

	ctx.ctx_metrics_manager = metrics_manager;
	ctx.ctx_metrics_manager.listen(done);
}


(function
main()
{
	var ctx = {
		ctx_cfgfile: mod_path.join(__dirname, '..', 'etc',
			'config.json'),
		ctx_cfgfile_notfound: 0,

		/*
		 * Holds the per-shard configuration for all gc workers
		 * processing delete records from this shard. This configuration
		 * includes the buckets that workers should read, in addition to
		 * tunables listed in the 'moray-cfg' schema in lib/schema.js.
		 */
		ctx_moray_cfgs: {},

		/*
		 * Holds a mapping from full shard domain to the client that
		 * each worker uses send RPCs to particular shards.
		 */
		ctx_moray_clients: {}
	};

	var log = ctx.ctx_log = mod_bunyan.createLogger({
		name: 'garbage-collector',
		level: process.env.LOG_LEVEL || mod_bunyan.INFO,
		serializers: mod_bunyan.stdSerializers
	});

	mod_vasync.pipeline({ arg: ctx, funcs: [
		/*
		 * Load the configuration file. This contains all requisite
		 * information for creating the clients below.
		 */
		load_config,

		/*
		 * If the collector received an outdated configuration,
		 * translate ctx.ctx_cfg appropriately.
		 */
		translate_config,

		/*
		 * Pull in log-related configuration. For now, this just sets
		 * the log-level according to what may or may not be in the
		 * config.
		 */
		setup_log,

		/*
		 * Set all global context fields. These are fields that hang
		 * directly off of the ctx object, and not any of it's
		 * sub-objects.
		 */
		set_global_ctx_fields,

		/*
		 * Create metrics manager and install application level
		 * collectors.
		 */
		setup_metrics,

		/*
		 * Create a SAPI sdc-client.
		 */
		setup_sapi_client,

		/*
		 * Load 'manta' application object.
		 */
		load_manta_application,

		/*
		 * Create one manta client.
		 */
		setup_manta_client,

		/*
		 * Create the gc manager
		 */
		mod_gc_manager.create_gc_manager,

		/*
		 * Create the restify server used for exposing configuration to
		 * the operator.
		 */
		lib_http_server.create_http_server
	] }, function (err) {
		if (err) {
			log.fatal(err, 'startup failure');
			process.exit(1);
		}

		log.info('startup complete');
	});
})();
