/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var mod_assertplus = require('assert-plus');
var mod_jsprim = require('jsprim');
var mod_util = require('util');
var mod_uuidv4 = require('uuid/v4');
var mod_vasync = require('vasync');
var mod_verror = require('verror');

var lib_testcommon = require('./common');

var mod_gc_manager = require('../lib/gc_manager');

var TEST_OWNER = mod_uuidv4();


function
do_basic_gc_manager_test(test_done)
{
	mod_vasync.waterfall([
		function create_context(next) {
			var opts = {
			    skip_moray_clients: true
			};
			lib_testcommon.create_mock_context(opts, function (err, ctx) {
				ctx.ctx_cfg.allowed_creators = [
					{
						uuid: TEST_OWNER
					}
				];
				next(err, ctx);
			});
		},
		function create_gc_manager(ctx, next) {
			mod_gc_manager.create_gc_manager(ctx, function () {
				ctx.ctx_gc_manager.once('running', function () {
					next(null, ctx);
				});
			});
		},
		function verify_gc_manager(ctx, next) {
			var manager = ctx.ctx_gc_manager;

			for (var i = 0; i < ctx.ctx_cfg.shards.length; i++) {
				var shard = ctx.ctx_cfg.shards[i];
				var buckets = ctx.ctx_cfg.buckets;
				var workers = manager.get_workers();

				mod_assertplus.ok(workers[shard.host], 'missing workers ' +
					'for shard.');

				buckets.forEach(function (bucket) {
					var bucket_name = bucket.name;
					var concurrency = ctx.ctx_cfg.concurrency;

					if (concurrency > 0) {
						mod_assertplus.ok(workers[shard.host][bucket_name],
							'missing workers for bucket.');
						mod_assertplus.equal(concurrency,
							workers[shard.host][bucket_name].length,
							'worker mismatch: expected ' + concurrency +
							'but found ' + workers[shard.host][
								bucket_name].length);
					}
				});
			}

			next(null, ctx);
		},
		function pause_workers(ctx, next) {
			var manager = ctx.ctx_gc_manager;

			manager.pause_workers(Object.keys(ctx.ctx_moray_clients),
				function () {
				var workers = manager.gcm_workers;

				Object.keys(workers).forEach(function (shard) {
					var buckets = workers[shard];
					Object.keys(buckets).forEach(function (bucket) {
						workers[shard][bucket].forEach(function (worker) {
							mod_assertplus.ok(worker.isInState('paused'),
								'worker is not paused');
						});
					});
				});

				next(null, ctx);
			});
		},
		function resume_workesr(ctx, next) {
			var manager = ctx.ctx_gc_manager;

			manager.resume_workers(Object.keys(ctx.ctx_moray_clients),
				function () {
				var workers = manager.gcm_workers;

				Object.keys(workers).forEach(function (shard) {
					var buckets = workers[shard];
					Object.keys(buckets).forEach(function (bucket) {
						workers[shard][bucket].forEach(function (worker) {
							mod_assertplus.ok(worker.isInState('running'),
								'worker is not running');
						});
					});
				});

				next(null, ctx);
			});
		}
	], function (err) {
		if (err) {
			process.exit(1);
		}
		test_done();
	});
}

mod_vasync.pipeline({ funcs: [
	function (_, next) {
		do_basic_gc_manager_test(next);
	}
]}, function (err) {
	if (err) {
		process.exit(1);
	}
	process.exit(0);
});
