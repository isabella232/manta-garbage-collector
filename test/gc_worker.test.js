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

var NUM_TEST_OBJECTS = 25;
var DELAY = 45000;

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


function
do_gc_worker_basic_test(test_done)
{
	mod_vasync.waterfall([
		function create_context(next) {
			lib_testcommon.create_mock_context(next);
		},
		function create_gc_worker(ctx, next) {
			var shard = Object.keys(ctx.ctx_moray_clients)[0];
			worker = lib_testcommon.create_gc_worker(ctx, shard, ctx.ctx_log);
			next(null, ctx, worker, shard)
		},
		function create_records(ctx, worker, shard, next) {
			var client = ctx.ctx_moray_clients[shard];

			mod_vasync.forEachParallel({
				inputs: TEST_OBJECTIDS,
				func: function create_record(objectid, done) {
					lib_testcommon.create_fake_delete_record(ctx, client,
						TEST_OWNER, objectid, TEST_SHARKS, done);
				}
			}, function (err) {
				setTimeout(function () {
					next(err, ctx, worker, shard);
				}, DELAY);
			});
		},
		function check_records_removed(ctx, worker, shard, next) {
			var client = ctx.ctx_moray_clients[shard];

			mod_vasync.forEachParallel({
				inputs: TEST_OBJECTIDS,
				func: function check_record_removed(objectid, done) {
					var key = mod_path.join(TEST_OWNER, objectid);
					lib_testcommon.get_fake_delete_record(client,
						key, function (err) {
						mod_assertplus.ok(err, 'expected ' +
							'delete record to be removed');
						mod_assertplus.ok(mod_verror.hasCauseWithName(
							err, 'ObjectNotFoundError'),
							'expected missing ' +
							'ObjectNotFoundError');
						done();
					});
				}
			}, function (err) {
				next(err, ctx, worker, shard);
			});

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
		test_done();
	});
}

function
do_gc_worker_pause_test(test_done)
{
	mod_vasync.waterfall([
		function create_context(next) {
			lib_testcommon.create_mock_context(next);
		},
		function create_gc_worker(ctx, next) {
			var shard = Object.keys(ctx.ctx_moray_clients)[0];
			worker = lib_testcommon.create_gc_worker(ctx, shard, ctx.ctx_log);
			worker.emit('pause');
			/*
			 * Pause the worker - no delete records should be
			 * removed.
			 */
			setTimeout(function () {
				next(null, ctx, worker, shard);
			}, 25000);
		},
		function create_records(ctx, worker, shard, next) {
			var client = ctx.ctx_moray_clients[shard];

			mod_vasync.forEachParallel({
				inputs: TEST_OBJECTIDS,
				func: function create_record(objectid, done) {
					lib_testcommon.create_fake_delete_record(ctx, client,
						TEST_OWNER, objectid, TEST_SHARKS, done);
				}
			}, function (err) {
				next(err, ctx, worker, shard);
			});
		},
		function check_records_not_yet_removed(ctx, worker, shard, next) {
			var client = ctx.ctx_moray_clients[shard];
			/*
			 * Wait a while and make sure the records haven't been
			 * removed -- the worker was paused when they were
			 * created.
			 */
			setTimeout(function () {
				mod_vasync.forEachParallel({
					inputs: TEST_OBJECTIDS,
					func: function check_record_exists(objectid, done) {
						var key = mod_path.join(TEST_OWNER, objectid);
						lib_testcommon.get_fake_delete_record(client,
							key, function (err, obj) {
							if (err) {
								ctx.ctx_log.error({
									err: err
								}, 'error reading record');
							}
							mod_assertplus.ok(obj, 'missing object');
							done();
						});
					}
				}, function (err) {
					/*
					 * Resume the worker, wait, and then
					 * check that the records were
					 * processed.
					 */
					worker.emit('resume');
					setTimeout(function () {
						next(err, ctx, worker, shard);
					}, DELAY);
				});
			}, DELAY);
		},
		function check_records_removed(ctx, worker, shard, next) {
			var client = ctx.ctx_moray_clients[shard];

			mod_vasync.forEachParallel({
				inputs: TEST_OBJECTIDS,
				func: function check_record_removed(objectid, done) {
					var key = mod_path.join(TEST_OWNER, objectid);
					lib_testcommon.get_fake_delete_record(client,
						key, function (err) {
						mod_assertplus.ok(err, 'expected ' +
							'delete record to be removed');
						mod_assertplus.ok(mod_verror.hasCauseWithName(
							err, 'ObjectNotFoundError'),
							'expected missing ' +
							'ObjectNotFoundError');
						done();
					});
				}
			}, function (err) {
				next(err, ctx, worker, shard);
			});

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
		test_done();
	});
};

mod_vasync.pipeline({ funcs: [
	function (_, next) {
		do_gc_worker_basic_test(next);
	},
	function (_, next) {
		do_gc_worker_pause_test(next);
	}
]}, function (err) {
	if (err) {
		process.exit(1);
	}
	console.log('test passed');
	process.exit(0);
});
