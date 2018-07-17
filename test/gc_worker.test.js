/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var mod_assertplus = require('assert-plus');
var mod_path = require('path');
var mod_uuidv4 = require('uuid/v4');
var mod_vasync = require('vasync');
var mod_verror = require('verror');

var lib_testcommon = require('./common');

/*
 * This is a basic end-to-end test for the gc pipeline. We create a worker, then
 * create an object in a postgres delete queue, and check that the worker
 * correctly cleans up the object.
 */

var TEST_OWNER = mod_uuidv4();

var NUM_TEST_OBJECTS = 10;
var DELAY = 45000;
var SHORT_DELAY = 5000;

var TEST_OBJECTIDS = (function generate_test_objectids() {
	var objectids = [];

	for (var i =  0; i < NUM_TEST_OBJECTS; i++) {
		objectids.push(mod_uuidv4());
	}

	return (objectids);
})();

var TEST_SHARKS = [
	'1.stor.orbit.example.com',
	'2.stor.orbit.example.com'
];

var TEST_INSTRUCTIONS = (function generate_test_instructions() {
	var instrs = {};
	for (var i = 0; i < TEST_SHARKS.length; i++) {
		instrs[TEST_SHARKS[i]] = TEST_OBJECTIDS.map(function (objectid) {
			return ([TEST_OWNER, objectid]);
		});
	}
	return (instrs);
})();


(function
do_gc_worker_basic_test(test_done)
{
	mod_vasync.waterfall([
		function create_context(next) {
			lib_testcommon.create_mock_context(function (err, ctx) {
				ctx.ctx_cfg.creators = [
					{
						uuid: TEST_OWNER
					}
				];
				next(err, ctx);
			});
		},
		function create_gc_worker(ctx, next) {
			var shard = Object.keys(ctx.ctx_moray_clients)[0];
			worker = lib_testcommon.create_gc_worker(ctx, shard,
				lib_testcommon.MANTA_FASTDELETE_QUEUE,
				ctx.ctx_log);
			worker.once('running', function () {
				next(null, ctx, worker, shard)
			});
		},
		function create_records(ctx, worker, shard, next) {
			var client = ctx.ctx_moray_clients[shard];

			mod_vasync.forEachParallel({
				inputs: TEST_OBJECTIDS,
				func: function create_record(objectid, done) {
					lib_testcommon.create_fake_delete_record(ctx, client,
						lib_testcommon.MANTA_FASTDELETE_QUEUE,
						TEST_OWNER, objectid, TEST_SHARKS, done);
				}
			}, function (err) {
				setTimeout(function () {
					next(err, ctx, worker, shard);
				}, SHORT_DELAY);
			});
		},
		function check_records_removed(ctx, worker, shard, next) {
			var client = ctx.ctx_moray_clients[shard];

			function check_removed() {
				mod_vasync.forEachParallel({
					inputs: TEST_OBJECTIDS,
					func: function check_record_removed(objectid, done) {
						var key = mod_path.join(TEST_OWNER, objectid);
						lib_testcommon.get_fake_delete_record(client,
							key, function (err) {
							if (!err) {
								ctx.ctx_log.debug('delete queue ' +
									'still has entries!');
							}
							done(err);
						});
					}
				}, function (_, results) {
					if (results.successes.length !== 0) {
						setTimeout(check_removed, SHORT_DELAY);
						return;
					}
					next(null, ctx, worker, shard);
				});
			}

			setImmediate(check_removed);
		},
		function check_instrs_uploaded(ctx, worker, shard, next) {
			var client = ctx.ctx_manta_client;
			lib_testcommon.find_instrs_in_manta(client, TEST_INSTRUCTIONS,
				ctx.ctx_mako_cfg.instr_upload_path_prefix,
				function (err) {
				if (err) {
					ctx.ctx_log.error(err, 'failed to find ' +
						'instructions in manta');
				}
				next(err);
			});
		}
	], function (err) {
		if (err) {
			process.exit(1);
		}
		process.exit(0);
	});
})();
