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
var mod_moray = require('moray');
var mod_verror = require('verror');
var mod_vasync = require('vasync');

var VE = mod_verror.VError;
var MorayDeleteRecordReader = require('../lib/moray_delete_record_reader').MorayDeleteRecordReader;

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

		setImmediate(done, null, out);
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

	load_test_config(function (err, cfg) {
		if (err) {
			ctx.ctx_log.error(err, 'unable to load test config');
			process.exit(1);
		}

		ctx.ctx_moray_clients = {};
		ctx.ctx_moray_cfgs = {};

		mod_vasync.forEachPipeline({
			inputs: cfg.shards,
			func: function create_client(cfg, next) {
				var shard = cfg.srvDomain || cfg.host;
				cfg.log = ctx.ctx_log;
				var client = mod_moray.createClient(cfg);

				client.once('connect', function () {
					client.removeAllListeners('error');
					ctx.ctx_moray_clients[shard] = client;
					next();
				});

				client.once('error', function (err) {
					client.removeAllListeners('connect');
					next(err);
				});
			}
		}, function (err) {
			if (err) {
				ctx.ctx_log.error('unable to created moray client "%s"',
					err.message);
				done(err);
				return;
			}
			done(null, ctx);
		});
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
create_fake_delete_record(ctx, client, owner, objectId, done)
{
	var value = {
		dirname: 'manta_gc_test',
		key: owner + '/' + objectId,
		headers: {},
		mtime: Date.now(),
		name: 'manta_gc_test_obj',
		creator: owner,
		owner: owner,
		objectId: objectId,
		roles: [],
		type: 'object',
		vnode: 1234
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

	create_fake_delete_record: create_fake_delete_record,

	remove_fake_delete_record: remove_fake_delete_record,

	MANTA_FASTDELETE_QUEUE: MANTA_FASTDELETE_QUEUE,

	MANTA_DELETE_LOG: MANTA_DELETE_LOG
};
