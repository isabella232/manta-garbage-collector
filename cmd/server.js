/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
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
		var out;
		try {
			out = JSON.parse(data.toString('utf8'));
		} catch (e) {
			done(new VE(e, 'error parsing file "%s"',
				ctx.ctx_cfgfile));
			return;
		}

		ctx.ctx_log.info('loaded configuration file "%s"',
			ctx.ctx_cfgfile);

		var schema_err = mod_schema.validate_shards_cfg(
			out.shards);
		if (schema_err) {
			done(new VE(schema_err, 'malformed shards ' +
				'configuration'));
			return;
		}

		schema_err = mod_schema.validate_creators_cfg(
			out.creators);
		if (schema_err) {
			done(new VE(schema_err, 'malformed creators config'));
			return;
		}

		schema_err = mod_schema.validate_moray_cfg(
			out.params.moray);
		if (schema_err) {
			done(new VE(schema_err, 'malformed moray ' +
				'configuration'));
			return;
		}

		schema_err = mod_schema.validate_mako_cfg(
			out.params.mako);
		if (schema_err) {
			done(new VE(schema_err, 'malformed mako ' +
				'configuration'));
			return;
		}

		ctx.ctx_cfg = out;

		setImmediate(done);
	});
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
load_index_shard_range(ctx, done)
{
	var index_shards = ctx.ctx_manta_app.metadata['INDEX_MORAY_SHARDS'];
	mod_assertplus.array(index_shards, 'index_shards');
	mod_assertplus.ok(ctx.ctx_cfg.index_shard_lo === undefined,
		'unexpected context configuration: \'index_shard_lo\'');
	mod_assertplus.ok(ctx.ctx_cfg.index_shard_hi === undefined,
		'unexpected context configuration: \'index_shard_hi\'');

	if (index_shards.length === 0) {
		done(new VE('Manta application has no index shards.'));
		return;
	}

	/*
	 * Today, garbage-collector instances are assigned shards in contiguous
	 * inclusive ranges like [2,8]. This assignment scheme relies on the
	 * convention that index shards in production deployments are given
	 * consecutive numeric shard names: 2.moray.{{DOMAIN}},
	 * 3.moray.{{DOMAIN}} ... 97.moray.{{DOMAIN}}. In order to ensure that
	 * the collector doesn't attempt to process records for non-index
	 * shards, it loads the complete index shard range from the SAPI Manta
	 * application below.
	 *
	 * The list of shards in the SAPI Manta application's INDEX_MORAY_SHARDS
	 * metadata array are not ordered. It is also not the case that the
	 * shard whose name is the highest numeric value in the list contains
	 * the "last" field (the "last" field is syntax required by hogan.js
	 * template engine).
	 *
	 * In order to correctly filter operator requests to GC from shards that
	 * are not index shards, we retrieve the full range of index shards by
	 * finding the lowest and highest valued numeric shard names in the
	 * list. Every time a request to change the range of shards a collector
	 * should GC from is made, a check is done to ensure the new range is a
	 * subset of the range of full index shards retrieved below.
	 *
	 * There is future work planned to remove the assumption of consecutive
	 * numeric index shard names from the garbage-collector.
	 */
	var shard_url_re = new RegExp('^(\\d+).' +
		ctx.ctx_cfg.shards.domain_suffix + '$');

	function parse_shard_number_from_url(shard_url, cb) {
		var results = shard_url_re.exec(shard_url);
		if (results === null) {
			cb(new VE('Unexpected shard url \'%s\'. Collector ' +
				'expects shard names matching \'%s\'.',
				shard_url, shard_url_re.toString()));
			return;
		}
		/*
		 * RegExp.exec puts places the single matched group at index
		 * 1 of the 'result' array.
		 */
		var shard_num = parseInt(results[1], 10);

		if (isNaN(shard_num)) {
			cb(new VE('Unexpected shard url \'%s\', collector ' +
				'expects consecutive numeric shard names ' +
				'matching \'%s\'.', shard_url,
				shard_url_re.toString()));
			return;
		}

		cb(null, shard_num);
	}

	function parse_shard_url_and_update_range(shard, next) {
		parse_shard_number_from_url(shard.host,
			function (err, shard_num) {
			if (err) {
				next(err);
				return;
			}
			if (ctx.ctx_cfg.index_shard_lo === undefined ||
			    ctx.ctx_cfg.index_shard_lo > shard_num) {
				ctx.ctx_cfg.index_shard_lo = shard_num;
			}

			if (ctx.ctx_cfg.index_shard_hi === undefined ||
			    ctx.ctx_cfg.index_shard_hi < shard_num) {
				ctx.ctx_cfg.index_shard_hi = shard_num;
			}
			next();
		});
	}

	mod_vasync.forEachPipeline({
		inputs: index_shards,
		func: parse_shard_url_and_update_range
	}, function (err) {
		if (err) {
			done(err);
			return;
		}
		ctx.ctx_log.info({
			lo: ctx.ctx_cfg.index_shard_lo,
			hi: ctx.ctx_cfg.index_shard_hi
		}, 'Discovered index shard range');
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

	ctx.ctx_mako_cfg = mod_jsprim.deepCopy(ctx.ctx_cfg.params.mako);
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
		 * We only allow gc to process records on index shards. These
		 * are the only shards that have a Manta table.
		 */
		load_index_shard_range,

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
