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

/*
 * We create an object with this user to ensure that the 'creators' filter in
 * the garbage collector's configuration is correctly applied.
 */
var TEST_OTHER_OWNER = mod_uuidv4();
var TEST_OTHER_OBJECTID = mod_uuidv4();

var BUCKET = lib_testcommon.MANTA_FASTDELETE_QUEUE;
var OBJECTS = (function generate_object_spec() {
	var objects = {};
	objects[TEST_OWNER] = TEST_OBJECTID;
	objects[TEST_OTHER_OWNER] = TEST_OTHER_OBJECTID;

	return (objects);
})();
var DELAY = 5000;
var LONG_DELAY = 16000;

var SHARK = '1.stor.orbit.example.com';


function
do_basic_test(test_done)
{
	mod_vasync.waterfall([
		function setup_context(next) {
			lib_testcommon.create_mock_context(function (err, ctx) {
				if (err) {
					console.log('error creating context');
					next(err);
					return;
				}

				ctx.ctx_cfg.creators = [
					{
						uuid: TEST_OWNER
					}
				];

				var shard = Object.keys(ctx.ctx_moray_clients)[0];

				next(null, ctx, shard);
			});
		},
		function create_delete_records(ctx, shard, next) {
			mod_vasync.forEachParallel({
				inputs: Object.keys(OBJECTS),
				func: function (owner, done) {
					lib_testcommon.create_fake_delete_record(
						ctx,
						ctx.ctx_moray_clients[shard],
						BUCKET,
						owner,
						OBJECTS[owner],
						[SHARK],
						done
					);
				}
			}, function () {
				next(null, ctx, shard);
			});
		},
		function read_delete_record(ctx, shard, next) {
			var expected_key = mod_path.join(TEST_OBJECTID, SHARK);
			var listener = new mod_events.EventEmitter();
			var reader = lib_testcommon.create_moray_delete_record_reader(ctx,
				shard, listener);

			var timer = setTimeout(function () {
				listener.removeAllListeners('record');
				mod_assertplus.ok(false, 'did not receive record event');
			}, DELAY);

			listener.once('record', function (record) {
				mod_assertplus.notEqual(TEST_OTHER_OWNER, record.value.creator,
					'received record from unexpected owner');
				mod_assertplus.equal(TEST_OWNER, record.value.creator,
					'received record from unexpected owner');

				if (record.key === expected_key) {
					clearTimeout(timer);
					reader.emit('shutdown');
					next(null, ctx, shard);
				} else {
					ctx.ctx_log.debug({
						expected: expected_key,
						received: record.key
					}, 'record key mismatch');
				}
			});
		},
		function remove_fake_delete_record(ctx, shard, next) {
			mod_vasync.forEachParallel({
				inputs: Object.keys(OBJECTS),
				func: function (owner, done) {
					lib_testcommon.remove_fake_delete_record(
						ctx,
						ctx.ctx_moray_clients[shard],
						mod_path.join(OBJECTS[owner], SHARK),
						done
					);
				}
			}, next);
		}
	], function (err) {
		if (err) {
			process.exit(1);
		}
		console.log('tests passed');
		test_done();
	});
}


function
do_error_test(test_done)
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

				ctx.ctx_cfg.creators = [
					{
						uuid: TEST_OWNER
					}
				];

				var client = ctx.ctx_moray_clients[shard];
				delete (ctx.ctx_moray_clients[shard]);

				next(null, ctx, shard, client);
			});
		},
		function create_reader(ctx, shard, client, next) {
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
		function create_fake_record(ctx, shard, client, reader, listener,
			next) {
			lib_testcommon.create_fake_delete_record(
				ctx,
				client,
				BUCKET,
				TEST_OWNER,
				OBJECTS[TEST_OWNER],
				[SHARK],
				function (err) {
				if (err) {
					ctx.ctx_log.error(err, 'unabled to create delete record');
					next(err);
					return;
				}
				next(null, ctx, shard, client, reader, listener);
			});
		},
		function give_back_moray_client(ctx, shard, client, reader, listener, next) {
			var received_record = false;
			listener.once('record', function (record) {
				mod_assertplus.equal(record.value.creator, TEST_OWNER,
					'unexpected creator');
				mod_assertplus.equal(mod_path.join(TEST_OBJECTID, SHARK),
					record.key, 'unexpected key');
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
				lib_testcommon.remove_fake_delete_record(
					ctx,
					client,
					mod_path.join(TEST_OBJECTID, SHARK),
					function (err) {
					next(err, ctx, reader);
				});
			}, LONG_DELAY);
		},
		function shutdown_reader(ctx, reader, next) {
			reader.emit('shutdown');
			next();
		}
	], function (err) {
		if (err) {
			process.exit(1);
		}
		test_done();
	});
}


function
do_filter_test(test_done)
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

				ctx.ctx_cfg.creators = [
					{
						uuid: TEST_OWNER
					}
				];

				var client = ctx.ctx_moray_clients[shard];
				delete (ctx.ctx_moray_clients[shard]);

				next(null, ctx, shard, client);
			});
		},
		function create_fake_delete_record(ctx, shard, client, next) {
			lib_testcommon.create_fake_delete_record(
				ctx,
				client,
				BUCKET,
				TEST_OTHER_OWNER,
				OBJECTS[TEST_OTHER_OWNER],
				[SHARK],
				function (err) {
				if (err) {
					ctx.ctx_log.error(err, 'unabled to create delete record');
					next(err);
					return;
				}
				next(null, ctx, shard, client);
			});
		},
		function create_reader(ctx, shard, client, next) {
			var listener = new mod_events.EventEmitter();
			var reader = lib_testcommon.create_moray_delete_record_reader(
				ctx, shard, listener);

			listener.on('record', function (record) {
				mod_assertplus.notEqual(record.value.creator,
					TEST_OTHER_OWNER, 'received record with ' +
					'unexpected creator');
			});

			setTimeout(function () {
				next(null, ctx, shard, client);
			}, DELAY);
		},
		function remove_record(ctx, shard, client, next) {
			lib_testcommon.remove_fake_delete_record(
				ctx,
				client,
				mod_path.join(TEST_OTHER_OBJECTID, SHARK),
				next);
		}
	], function (err) {
		test_done(err)
	});
}


mod_vasync.pipeline({ funcs: [
	function (_, next) {
		do_basic_test(next);
	},
	function (_, next) {
		do_error_test(next);
	},
	function (_, next) {
		do_filter_test(next);
	}
]}, function (err) {
	if (err) {
		process.exit(1);
	}
	process.exit(0);
});
