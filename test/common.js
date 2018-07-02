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
var mod_path = require('path');
var mod_manta = require('manta');
var mod_moray = require('moray');
var mod_verror = require('verror');
var mod_vasync = require('vasync');

var VE = mod_verror.VError;
var MorayDeleteRecordReader = require('../lib/moray_delete_record_reader').MorayDeleteRecordReader;
var MorayDeleteRecordCleaner = require('../lib/moray_delete_record_cleaner').MorayDeleteRecordCleaner;
var MakoInstructionUploader = require('../lib/mako_instruction_uploader').MakoInstructionUploader;
var DeleteRecordTransformer = require('../lib/delete_record_transformer').DeleteRecordTransformer;

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
create_mock_context(done)
{
	var ctx = {};

	ctx.ctx_log = mod_bunyan.createLogger({
		name: 'Test',
		level: process.env.LOG_LEVEL || 'info'
	});

	mod_vasync.waterfall([
		function load_config(next) {
			load_test_config(function (err, cfg) {
				ctx.ctx_cfg = cfg;
				next(err, cfg);
			});
		},
		function create_moray_clients(cfg, next) {
			ctx.ctx_moray_cfgs = {};
			ctx.ctx_moray_clients = {};

			mod_vasync.forEachPipeline({
				inputs: cfg.shards,
				func: function create_client(cfg, cb) {
					var shard = cfg.srvDomain || cfg.host;
					cfg.log = ctx.ctx_log;
					var client = mod_moray.createClient(cfg);

					client.once('connect', function () {
						client.removeAllListeners('error');
						ctx.ctx_moray_clients[shard] = client;
						cb();
					});

					client.once('error', function (err) {
						client.removeAllListeners('connect');
						cb(err);
					});
				}
			}, function (err) {
				if (err) {
					ctx.ctx_log.error('unable to create moray client "%s"',
						err.message);
					next(err);
					return;
				}
				next(null, cfg);
			});
		},
		function create_manta_client(cfg, next) {
			ctx.ctx_manta_client = mod_manta.createClient(cfg.manta);
			next();
		}
	], function (err) {
		if (err) {
			console.log.error(err, 'unable to created mock context');
		}
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
	}

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
	}

	return (new MorayDeleteRecordCleaner(opts));
}


function
create_mako_instruction_uploader(ctx, listener)
{
	ctx.ctx_mako_cfg = {
		instr_upload_batch_size: 1,
		instr_path_prefix: mod_path.join('/', ctx.ctx_cfg.manta.user,
			'stor', 'manta_gc', 'mako')
	}
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
create_fake_delete_record(ctx, client, owner, objectId, done)
{
	var value = {
		dirname: 'manta_gc_test',
		key: mod_path.join(owner, objectId),
		headers: {},
		mtime: Date.now(),
		name: 'manta_gc_test_obj',
		creator: owner,
		owner: owner,
		objectId: objectId,
		roles: [],
		type: 'object',
		vnode: 1234,
		contentLength: 0
	};
	client.putObject(MANTA_FASTDELETE_QUEUE, value.key,
		value, {}, function (err) {
		if (err) {
			ctx.ctx_log.error(err, 'unable to create test object');
			process.exit(1);
		}
		done();
	});
}


function
remove_fake_delete_record(ctx, client, key, done)
{
	client.delObject(MANTA_FASTDELETE_QUEUE, key, {}, done);
}



module.exports = {
	create_mock_context: create_mock_context,

	create_moray_delete_record_reader: create_moray_delete_record_reader,

	create_moray_delete_record_cleaner: create_moray_delete_record_cleaner,

	create_mako_instruction_uploader: create_mako_instruction_uploader,

	create_delete_record_transformer: create_delete_record_transformer,

	create_fake_delete_record: create_fake_delete_record,

	remove_fake_delete_record: remove_fake_delete_record,

	MANTA_FASTDELETE_QUEUE: MANTA_FASTDELETE_QUEUE,

	MANTA_DELETE_LOG: MANTA_DELETE_LOG
};
