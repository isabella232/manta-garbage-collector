/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2019, Joyent, Inc.
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
var DELAY = 50000;
var SHORT_DELAY = 10000;

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
			lib_testcommon.create_mock_context({}, function (err, ctx) {
				ctx.ctx_cfg.allowed_creators = [
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
					setTimeout(function () {
						next(null, ctx, worker, shard);
					}, DELAY);
				});
			}

			setImmediate(check_removed);
		},
		function check_instrs_uploaded(ctx, worker, shard, next) {
			var client = ctx.ctx_manta_client;
			lib_testcommon.find_instrs_in_manta(client, TEST_INSTRUCTIONS,
				ctx.ctx_cfg.tunables.instr_upload_path_prefix,
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
}


function
do_gc_worker_control_test(test_done)
{
	mod_vasync.waterfall([
		function create_context(next) {
			lib_testcommon.create_mock_context({}, function (err, ctx) {
				ctx.ctx_cfg.allowed_creators = [
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
		function pause_gc_worker(ctx, worker, shard, next) {
			var timer = setTimeout(function () {
				mod_assertplus.ok(false, 'did not receive ' +
					'pause event from GC worker');
			}, SHORT_DELAY);

			worker.once('paused', function () {
				clearTimeout(timer);
				next(null, ctx, worker, shard);
			});

			worker.pause();
		},
		function check_actors_paused(ctx, worker, shard, next) {
			worker.gcw_pipeline.forEach(function (actor) {
				mod_assertplus.ok(actor.isInState('paused'),
					'actor in non-paused state');
			});
			next(null, ctx, worker, shard);
		},
		function resume_gc_worker(ctx, worker, shard, next) {
			var timer = setTimeout(function () {
				mod_assertplus.ok(false, 'did not receive ' +
					'running event after resuming worker');
			}, SHORT_DELAY);

			worker.once('running', function () {
				clearTimeout(timer);
				next(null, ctx, worker, shard);
			});

			worker.resume();
		},
		function check_actors_running(ctx, worker, shard, next) {
			worker.gcw_pipeline.forEach(function (actor) {
				mod_assertplus.ok(actor.isInState('running'),
					'actor in non-running state');
			});
			next(null, ctx, worker, shard);
		},
		function shutdown_gc_workers(ctx, worker, shard, next) {
			var timer = setTimeout(function () {
				mod_assertplus.ok(false, 'did not receive' +
					'worker shutdown event');
			}, SHORT_DELAY);

			worker.once('shutdown', function () {
				clearTimeout(timer);
				next(null, ctx, worker, shard);
			});

			worker.shutdown();
		},
		function check_actors_shutdown(ctx, worker, shard, next) {
			worker.gcw_pipeline.forEach(function (actor) {
				mod_assertplus.ok(actor.isInState('shutdown'),
					'actor in non-shutdown state');
			});

			next(null, ctx, worker, shard);
		}
	], function (err) {
		if (err) {
			process.exit(1);
		}
		test_done();
	});
}


mod_vasync.pipeline({ funcs: [
	/*
	function (_, next) {
		do_gc_worker_control_test(next);
	},
	*/
	function (_, next) {
		do_gc_worker_basic_test(next)
	}
]}, function (err) {
	if (err) {
		process.exit(1);
	}
	process.exit(0);
});
