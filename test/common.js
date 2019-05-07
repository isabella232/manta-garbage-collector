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
var mod_path = require('path');
var mod_moray = require('moray');
var mod_verror = require('verror');
var mod_vasync = require('vasync');

var VE = mod_verror.VError;
var MorayDeleteRecordReader = require('../lib/moray_delete_record_reader').MorayDeleteRecordReader;
var MorayDeleteRecordCleaner = require('../lib/moray_delete_record_cleaner').MorayDeleteRecordCleaner;
var MakoInstructionWriter = require('../lib/mako_instruction_writer').MakoInstructionWriter;
var DeleteRecordTransformer = require('../lib/delete_record_transformer').DeleteRecordTransformer;

var GCWorker = require('../lib/gc_worker').GCWorker;

var TEST_CONFIG_PATH = mod_path.join('..', 'etc', 'testconfig.json');

var MANTA_FASTDELETE_QUEUE = 'manta_fastdelete_queue';
var MANTA_DELETE_LOG = 'manta_delete_log';


function
load_test_config(done)
{
	mod_fs.readFile(TEST_CONFIG_PATH, function (err, data) {
		if (err) {
			done(err);
			return;
		}

		var out;
		try {
			out = JSON.parse(data.toString('utf8'));
		} catch (e) {
			done(new VE(err, 'loading test config "%s"', TEST_CONFIG_PATH));
			return;
		}

		done(null, out);
	});
}


function
create_mock_context(opts, done)
{
	var ctx = {};

	mod_assertplus.object(opts, 'opts');
	mod_assertplus.optionalBool(opts.skip_manta_client, 'opts.skip_manta_client');
	mod_assertplus.optionalBool(opts.skip_moray_clients, 'opts.skip_moray_clients');

	ctx.ctx_log = mod_bunyan.createLogger({
		name: 'Test',
		level: process.env.LOG_LEVEL || 'info'
	});

	mod_vasync.waterfall([
		function load_config(next) {
			load_test_config(function (err, cfg) {
				ctx.ctx_cfg = cfg;
				next(err);
			});
		},
		function create_moray_clients(next) {
			ctx.ctx_moray_clients = {};

			if (opts.skip_moray_clients) {
				next();
				return;
			}

			var barrier = mod_vasync.barrier();

			var shards = ctx.ctx_cfg.shards;

			mod_vasync.forEachPipeline({
				inputs: shards,
				func: function (shard, cb) {
					var moray_cfg = mod_jsprim.mergeObjects(ctx.ctx_cfg.moray,
						{ log: ctx.ctx_log, srvDomain: shard.host });

					var client = mod_moray.createClient(moray_cfg);

					client.once('connect', function () {
						ctx.ctx_moray_clients[shard.host] = client;
						cb();
					});

					client.once('error', function (err) {
						mod_assert.ok(false, 'error setting up test' +
							'context');
					});
				}
			}, function (err) {
				next(err);
			});
		},
	], function (err) {
		if (err) {
			console.log(err, 'unable to created mock context');
		}
		ctx.ctx_total_cache_entries = 0;
		done(err, ctx);
	});
}

function
create_moray_delete_record_reader(ctx, shard, listener)
{
	var opts = {
		ctx: ctx,
		bucket: MANTA_FASTDELETE_QUEUE,
		shard: shard,
		listener: listener,
		log: ctx.ctx_log,
	};

	return (new MorayDeleteRecordReader(opts));
}


function
create_moray_delete_record_cleaner(ctx, shard)
{
	var opts = {
		ctx: ctx,
		bucket: MANTA_FASTDELETE_QUEUE,
		shard: shard,
		log: ctx.ctx_log
	};

	return (new MorayDeleteRecordCleaner(opts));
}


function
create_mako_instruction_writer(ctx, listener)
{
	var opts = {
		ctx: ctx,
		log: ctx.ctx_log,
		listener: listener
	};

	return (new MakoInstructionUploader(opts))
}


function
create_delete_record_transformer(ctx, shard, listeners)
{
	var opts = {
		ctx: ctx,
		log: ctx.ctx_log,
		mako_listener: listeners.mako_listener,
		moray_listener: listeners.moray_listener
	};

	return (new DeleteRecordTransformer(opts));
}


function
create_fake_delete_record(ctx, client, bucket, owner, objectId, sharks, done)
{
	var key = mod_path.join(objectId, (sharks.length > 0) ? sharks[0] : owner);
	var value = {
		dirname: 'manta_gc_test',
		key: key,
		headers: {},
		mtime: Date.now(),
		name: 'manta_gc_test_obj',
		creator: owner,
		owner: owner,
		objectId: objectId,
		roles: [],
		type: 'object',
		vnode: 1234,
		sharks: sharks.map(function (storage_id) {
			return ({
				dc: 'dc',
				manta_storage_id: storage_id
			});
		}),
		contentLength: sharks.length > 0 ? 512000 : 0
	};
	client.putObject(bucket, value.key,
		value, {}, function (err) {
		if (err) {
			ctx.ctx_log.error(err, 'unable to create test object');
			process.exit(1);
		}
		done();
	});
}


function
get_fake_delete_record(client, key, done)
{
	client.getObject(MANTA_FASTDELETE_QUEUE, key, done);
}


function
remove_fake_delete_record(ctx, client, key, done)
{
	client.delObject(MANTA_FASTDELETE_QUEUE, key, {}, done);
}


function
create_gc_worker(ctx, shard, bucket, log) {
	var opts = {
		bucket: bucket,
		ctx: ctx,
		shard: shard,
		log: log
	};

	return (new GCWorker(opts));
}


/*
 * Given an object mapping storage ids to arrays identifying individual objects.
 * Verify that mako cleanup instructions have been written for those objects
 * and only appear once in the object data uploaded by this GC worker.
 */
function
find_instrs_in_manta(client, instrs, path_prefix, find_done)
{
	function get_instr_object_paths_for_storage_id(storage_id, cb) {
		var path = mod_path.join(path_prefix, storage_id);

		instr_paths = [];

		client.ls(path, {}, function (err, stream) {
			if (err) {
				find_done(new VE(err, 'error listing ' +
				    'path in Manta: ' + path));
				return;
			}
			stream.on('object', function (obj) {
				instr_paths.push(mod_path.join(path,
					obj.name));
			});

			stream.on('error', function (err) {
				console.log(err, 'unable to list ' +
					'mako instrs');
				cb(err);
			});

			stream.on('end', function () {
				cb(null, instr_paths);
			});
		});
	}

	function get_instr_object_data(paths, cb) {

		var chunks = [];

		function get_obj(path, get_cb) {
			client.get(path, {}, function (err, stream) {
				var contents = '';
				if (err) {
					get_cb(err);
					return;
				}
				stream.on('data', function (buf) {
					contents = contents.concat(
						buf.toString());
				});
				stream.on('end', function () {
					mod_assertplus.ok(contents,
						'inspecting empty object');
					chunks.push(contents);
					get_cb();
				});
				stream.on('error', function (err) {
					get_cb(err);
				});
			});
		}

		mod_vasync.forEachParallel({
			inputs: paths,
			func: get_obj
		}, function (err) {
			cb(err, chunks.join('\n'));
		});

	}

	function search_instr_object_data(data, search_strs, cb) {
		var occurrences = {};
		mod_vasync.forEachParallel({
			inputs: search_strs,
			func: function search(str, done) {
				var count = (data.match(new RegExp(str, "g")) ||
					[]).length;
				occurrences[str] = count;
				done();
			}
		}, function (err) {
			cb(err, occurrences);
		});
	}

	function find_instrs_for_storage_id(storage_id, done) {
		var results;

		var search_strs = instrs[storage_id].map(
			function (instr) {
			return instr.join('\t');
		});

		mod_vasync.waterfall([
			get_instr_object_paths_for_storage_id.bind(null, storage_id),
			get_instr_object_data,
			function (data, next) {
				search_instr_object_data(data, search_strs,
					function (err, res) {
					if (err) {
						next(err);
						return;
					}
					results = res;
					next();
				});
			}
		], function (err) {
			if (err) {
				done(err);
				return;
			}
			var errors = [];

			mod_vasync.forEachParallel({
				inputs: Object.keys(results),
				func: function (key, cb) {
					var count = results[key];
					/*
					 * It's possible that the same
					 * instructions are uploaded multiple
					 * times because the
					 * MorayDeleteRecordReader has wrapped
					 * around.
					 */
					if (count < 1) {
						errors.push(new mod_verror.VError(
							'instruction "%s" found "%d" ' +
							'times in instruction objects',
							key, count));
					}
					cb();
				}
			}, function (_) {
				if (errors.length > 0) {
					done(new mod_verror.MultiError(errors));
					return;
				}
				done();
			});
		});
	}

	mod_vasync.forEachPipeline({
		inputs: Object.keys(instrs),
		func: find_instrs_for_storage_id
	}, find_done);
}


module.exports = {
	create_mock_context: create_mock_context,

	create_moray_delete_record_reader: create_moray_delete_record_reader,

	create_moray_delete_record_cleaner: create_moray_delete_record_cleaner,

	create_mako_instruction_uploader: create_mako_instruction_uploader,

	create_delete_record_transformer: create_delete_record_transformer,

	create_gc_worker: create_gc_worker,

	create_fake_delete_record: create_fake_delete_record,

	remove_fake_delete_record: remove_fake_delete_record,

	get_fake_delete_record: get_fake_delete_record,

	find_instrs_in_manta: find_instrs_in_manta,

	load_test_config: load_test_config,

	MANTA_FASTDELETE_QUEUE: MANTA_FASTDELETE_QUEUE,

	MANTA_DELETE_LOG: MANTA_DELETE_LOG
};
