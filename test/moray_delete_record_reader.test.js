/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var mod_assertplus = require('assert-plus');
var mod_events = require('events');
var mod_path = require('path');
var mod_util = require('util');
var mod_uuidv4 = require('uuid/v4');
var mod_vasync = require('vasync');

var lib_testcommon = require('./common');


var TEST_OWNER = mod_uuidv4();
var TEST_OBJECTID = mod_uuidv4();
var DELAY = 3000;
var LONG_DELAY = 16000;


function
do_basic_test()
{
	mod_vasync.waterfall([
		function setup_context(next) {
			lib_testcommon.create_mock_context(function (err, ctx) {
				if (err) {
					console.log('error creating context');
					next(err);
					return;
				}

				var shard = Object.keys(ctx.ctx_moray_clients)[0];

				next(null, ctx, shard);
			});
		},
		function create_delete_record(ctx, shard, next) {
			lib_testcommon.create_fake_delete_record(ctx,
				ctx.ctx_moray_clients[shard],
				lib_testcommon.MANTA_FASTDELETE_QUEUE,
				TEST_OWNER, TEST_OBJECTID, [], function (err) {
				if (err) {
					ctx.ctx_log.error(err, 'unabled to create delete record');
					next(err);
					return;
				}
				next(null, ctx, shard);
			});
		},
		function read_delete_record(ctx, shard, next) {
			var key = mod_path.join(TEST_OWNER, TEST_OBJECTID);
			var listener = new mod_events.EventEmitter();
			var reader = lib_testcommon.create_moray_delete_record_reader(ctx,
				shard, listener);

			var timer = setTimeout(function () {
				listener.removeAllListeners('record');
				mod_assertplus.ok(false, 'did not receive record event');
				next(null, ctx, shard);
			}, DELAY);

			listener.once('record', function (record) {
				clearTimeout(timer);
				ctx.ctx_log.debug({
					received_key: record.key,
					expected_key: key,
					record: mod_util.inspect(record)
				}, 'received record');
				mod_assertplus.equal(record.key, key, 'unexpected key ' +
					'from record sent to listener');
				ctx.ctx_log.info('test passed');
				next(null, ctx, shard);
			});
		}, function remove_fake_delete_record(ctx, shard, next) {
			lib_testcommon.remove_fake_delete_record(ctx, ctx.ctx_moray_clients[shard],
				mod_path.join(TEST_OWNER, TEST_OBJECTID), next);
		}
	], function (err) {
		if (err) {
			process.exit(1);
		}
		console.log('tests passed');
		process.exit(0);
	});
}


function
do_error_test()
{
	mod_vasync.waterfall([
		function setup_context(next) {
			lib_testcommon.create_mock_context(function (err, ctx) {
				if (err) {
					console.log('error creating context');
					next(err);
					return;
				}

				var shard = Object.keys(ctx.ctx_moray_clients)[0];

				var client = ctx.ctx_moray_clients[shard];
				delete (ctx.ctx_moray_clients[shard]);

				next(null, ctx, shard, client);
			});
		},
		function create_reader(ctx, shard, client) {
			var listener = new mod_events.EventEmitter();
			var reader = lib_testcommon.create_moray_delete_record_reader(
				ctx, shard, listener);

			/*
			 * The record reader doesn't have access to a
			 * moray client right now. It shouldn't be
			 * sending records.
			 */
			listener.on('record', function (record) {
				mod_assertplus.ok(false, 'received unexpected moray ' +
					'record');
			});

			setTimeout(function() {
				listener.removeAllListeners('record');
				next(null, ctx, shard, client, reader, listener);
			}, LONG_DELAY);
		},
		function create_fake_record(ctx, shard, client, reader, listener) {
			lib_testcommon.create_fake_delete_record(ctx, client,
				lib_testcommon.MANTA_FASTDELETE_QUEUE, TEST_OWNER,
				TEST_OBJECTID, [], function (err) {
				if (err) {
					ctx.ctx_log.error(err, 'unabled to create delete record');
					next(err);
					return;
				}
				next(null, ctx, shard, client, reader, listener);
			});
		},
		function give_back_moray_client(ctx, shard, client, reader, listener) {
			var received_record = false;
			listener.on('record', function (record) {
				received_record = true;
			});
			/*
			 * The behavior we expect is that the record
			 * reader resumes sending records once the error
			 * condition has cleared.
			 */
			ctx.ctx_moray_clients[shard] = client;
			setTimeout(function () {
				mod_assertplus.ok(received_record, 'did not receive record ' +
					'after moray client returned');
				lib_testcommon.remove_fake_delete_record(ctx, client,
					mod_path.join(TEST_OWNER, TEST_OBJECTID), next);
			}, LONG_DELAY);
		}
	], function (err) {
		if (err) {
			process.exit(1);
		}
		console.log('tests passed');
		process.exit(0);
	});
}

do_basic_test();
do_error_test();
