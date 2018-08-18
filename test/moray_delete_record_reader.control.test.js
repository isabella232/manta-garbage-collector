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
do_pause_resume_test(test_done)
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
		function create_and_pause_reader(ctx, shard, next) {
			var listener = new mod_events.EventEmitter();
			var reader = lib_testcommon.create_moray_delete_record_reader(
				ctx, shard, listener);
			var timer = setTimeout(function () {
				mod_assertplus.ok(false, 'did not receive ' +
					'paused event');
			}, DELAY);
			reader.once('paused', function () {
				clearTimeout(timer);
				next(null, ctx, shard, reader, listener);
			});
			reader.emit('assertPause');
		},
		function create_fake_record(ctx, shard, reader, listener, next) {
			var client = ctx.ctx_moray_clients[shard];
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
				next(null, ctx, shard, reader, listener);
			});
		},
		function listen_for_record_while_paused(ctx, shard, reader, listener, next) {
			listener.on('record', function () {
				mod_assertplus.ok(false, 'unexpected record event while ' +
					'reader is paused');
			});
			setTimeout(function () {
				listener.removeAllListeners('record');
				next(null, ctx, shard, reader, listener);
			}, DELAY);
		},
		function resume_reader(ctx, shard, reader, listener, next) {
			setTimeout(function () {
				mod_assertplus.ok(false, 'did not receive running ' +
					'event after resuming reader');
			}, DELAY);
			reader.once('running', function () {
				next(null, ctx, shard, reader, listener);
			});
			reader.emit('assertResume');
		},
		function listen_for_records(ctx, shard, reader, listener, next) {
			var timer = setTimeout(function () {
				mod_assertplus.ok(false, 'did not receive record ' +
					'after resuming reader');
			}, DELAY);
			listener.once('record', function (record) {
				clearTimeout(timer);
				next(null, ctx, shard);
			});
		},
		function remove_record(ctx, shard, next) {
			var client = ctx.ctx_moray_clients[shard];
			lib_testcommon.remove_fake_delete_record(
				ctx,
				client,
				mod_path.join(TEST_OBJECTID, SHARK),
				next);
		}
	], function (err) {
		if (err) {
			process.exit(1);
		}
		test_done();
	});
}


mod_vasync.pipeline({ funcs:[
	function (_, next) {
		do_pause_resume_test(next);
	}
]}, function (err) {
	if (err) {
		process.exit(1);
	}
	process.exit(0);
});
