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
			lib_testcommon.create_mock_context(function (err, ctx) {
				ctx.ctx_cfg.shards.buckets = [
					{
						bucket: 'manta_fastdelete_queue',
						concurrency: 1
					}
				];
				ctx.ctx_cfg.creators = [
					{
						uuid: TEST_OWNER
					}
				];
				next(err, ctx);
			});
		},
		function create_gc_manager(ctx, next) {
			mod_gc_manager.create_gc_manager(ctx, function () {
				next(null, ctx);
			});
		},
		function verify_gc_manager(ctx, next) {
			var manager = ctx.ctx_gc_manager;
			var shard_interval = ctx.ctx_cfg.shards.interval;
			var domain_suffix = ctx.ctx_cfg.shards.domain_suffix;

			for (var i = shard_interval[0]; i < shard_interval[1]; i++) {
				var domain = [i, domain_suffix].join('.');
				var workers = manager.get_workers();
				var buckets = ctx.ctx_cfg.shards.buckets;

				mod_assertplus.ok(workers[domain], 'missing workers ' +
					'for shard.');

				buckets.forEach(function (bucket) {
					var name = bucket.bucket;
					var concurrency = bucket.concurrency;

					if (concurrency > 0) {
						mod_assertplus.ok(workers[domain][bucket],
							'missing workers for bucket.');
						mod_assertplus.equal(concurrency,
							workers[domain][bucket],
							'missing some workers!');
					}
				});
			}

			next(null, ctx);
		},
		function destroy_gc_worker(ctx, next) {
			var manager = ctx.ctx_gc_manager;
			var bucket = 'manta_fastdelete_queue';

			var update = {
				buckets: [
					{
						bucket: bucket,
						concurrency: 0
					}
				]
			};

			manager.setup_shards(update, function (err) {
				if (err) {
					mod_assertplus.ok(false, 'error updating shard ' +
						'configuration');
				}
				next(null, ctx);
			});
		},
		function check_worker_gone(ctx, next) {
			var manager = ctx.ctx_gc_manager;
			var shard_interval = ctx.ctx_cfg.shards.interval;

			for (var i = shard_interval[0]; i < shard_interval[1]; i++) {
				var domain = [i, domain_suffix].join('.');
				var workers = manager.get_workers();

				mod_assertplus.equal(workers[domain][bucket].length, 0,
					'unexpected workers left after destroy!');
			}

			next(null, ctx);
		},
		function add_new_shard(ctx, next) {
			var manager = ctx.ctx_gc_manager;

			var update = {
				interval: [1,2]
			};

			manager.setup_shards(update, function (err) {
				if (err) {
					mod_assertplus.ok(false, 'error updating shard ' +
						'configuration');
				}
				next(null, ctx);
			});
		},
		function check_new_shard_cfg(ctx, next) {
			var manager = ctx.ctx_gc_manager;
			var domain_suffix = ctx.ctx_cfg.shards.domain_suffix;
			var bucket = 'manta_fastdelete_queue';

			var new_domain = [1, domain_suffix].join('.');
			var domain = [2, domain_suffix].join('.');

			var workers = manager.get_workers();

			mod_assertplus.ok((Object.keys(workers).length === 0) ||
				workers[new_domain][bucket] === undefined,
				'workers created unexpectedly!');
			mod_assertplus.ok(ctx.ctx_moray_clients[new_domain],
				'missing client for new shard');
			mod_assertplus.ok(ctx.ctx_moray_cfgs[new_domain],
				'missing config for new shard');

			var new_shard_cfg = ctx.ctx_moray_cfgs[new_domain];
			var old_shard_cfg = ctx.ctx_moray_cfgs[domain];

			mod_assertplus.ok(mod_jsprim.deepEqual(old_shard_cfg, new_shard_cfg),
				'old and new shard configurations do not match');
			next(null, ctx);
		},
		function create_workers_both_shards(ctx, next) {
			var manager = ctx.ctx_gc_manager;

			var update = {
				buckets: [
					{
						bucket: 'manta_fastdelete_queue',
						concurrency: 1
					}
				]
			};

			manager.setup_shards(update, function (err) {
				if (err) {
					mod_assertplus.ok(false, 'error updating shard ' +
						'configuration');
				}
				next(null, ctx);
			});
		},
		function check_workers_created_for_both_shards(ctx, next) {
			var manager = ctx.ctx_gc_manager;
			var workers = manager.get_workers();
			var bucket = 'manta_fastdelete_queue';

			mod_vasync.forEachParallel({
				inputs: Object.keys(ctx.ctx_moray_cfgs),
				func: function (shard, done) {
					mod_assertplus.equal(workers[shard][bucket].length,
						1, 'missing worker!');
					done();
				}
			}, function (_) {
				next(null, ctx);
			});
		},
		function pause_workers(ctx, next) {
			var manager = ctx.ctx_gc_manager;

			manager.pause_workers(Object.keys(ctx.ctx_moray_cfgs),
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

			manager.resume_workers(Object.keys(ctx.ctx_moray_cfgs),
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
