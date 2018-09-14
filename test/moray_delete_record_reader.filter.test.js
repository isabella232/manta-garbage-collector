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
do_filter_test(test_done)
{
	mod_vasync.waterfall([
		function setup_context(next) {
			lib_testcommon.create_mock_context({}, function (err, ctx) {
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
		do_filter_test(next);
	}
]}, function (err) {
	if (err) {
		process.exit(1);
	}
	process.exit(0);
});
