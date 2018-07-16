/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var mod_assertplus = require('assert-plus');
var mod_fsm = require('mooremachine');
var mod_util = require('util');
var mod_vasync = require('vasync');
var mod_verror = require('verror');

var mod_gc_worker = require('./gc_worker');

var lib_common = require('./common');

var VE = mod_verror.VError;


function
GCManager(opts)
{
	var self = this;

	mod_assertplus.object(opts, 'opts');
	mod_assertplus.object(opts.ctx, 'opts.ctx');
	mod_assertplus.object(opts.ctx.ctx_log, 'opts.ctx.ctx_log');

	self.gcm_ctx = opts.ctx;
	self.gcm_ctx.manager = self;

	self.gcm_log = self.gcm_ctx.ctx_log.child({
		component: 'GCManager'
	});
	self.gcm_http_server = opts.ctx.ctx_http_server;

	/*
	 * A mapping from shard srvDomain to bucket, to array of GCWorkers that
	 * process delete records from the bucket on that shard:
	 *
	 * {
	 * 	'2.moray.orbit.example.com': {
	 * 		'manta_fastdelete_queue': [...],
	 * 		'manta_delete_log": [...]
	 * 	}
	 * }
	 */
	self.gcm_workers = {};

	mod_fsm.FSM.call(this, 'init');
}
mod_util.inherits(GCManager, mod_fsm.FSM);


GCManager.prototype.state_init = function
state_init(S)
{
	var self = this;

	self.ensure_gc_workers(function (err) {
		if (err) {
			self.gcm_log(err, 'unable to ensure ' +
				'all workers, retrying...');
			setTimeout(function () {
				S.gotoState('init');
			}, 1000);
			return;
		}
	});
};


GCManager.prototype.state_running = function
state_running(S)
{
	var self = this;

	self.gcm_log.debug('running state');
};


/*
 * Ensure that for each shard, we have created the appropriate number of
 * GCWorkers.
 */
GCManager.prototype.ensure_gc_workers = function
ensure_gc_workers(done)
{
	var self = this;

	mod_vasync.forEachParallel({
		inputs: Object.keys(self.gcm_ctx.ctx_moray_cfgs),
		func: function (shard, cb) {
			self._ensure_workers_for_shard(shard, cb);
		}
	}, function (err) {
		if (err) {
			self.gcm_log.warn({
				err: err
			}, 'error resolving workers');
		}
		done(err);
	});
};


GCManager.prototype._ensure_workers_for_bucket = function
_ensure_workers_for_bucket(shard, bucket, concurrency, ensure_done)
{
	var self = this;

	self.gcm_log.debug({
		shard: shard,
		bucket: bucket,
		num_workers: (self.gcm_workers[shard][bucket] || []).length,
		num_desired: concurrency
	}, 'ensuring workers');

	mod_vasync.whilst(
		function unresolved() {
			return (concurrency !==
			    (self.gcm_workers[shard][bucket] ||
			    []).length);
		},
		function create_or_destroy_workers(next) {
			if ((self.gcm_workers[shard][bucket] ||
				[]).length < concurrency) {
				self._create_gc_worker(shard, bucket, next);
			} else {
				self._destroy_gc_worker(shard, bucket, next);
			}
		},
		function whilst_done(err) {
			if (err) {
				self.gcm_log.warn({
					shard: shard,
					bucket: bucket,
					err: err
				}, 'error while resolving workers');
			}
			ensure_done(err);
		});
};


GCManager.prototype._ensure_workers_for_shard = function
_ensure_workers_for_shard(shard, ensure_done)
{
	var self = this;

	var shard_cfg = self.gcm_ctx.ctx_moray_cfgs[shard];

	if (!self.gcm_workers[shard]) {
		self.gcm_workers[shard] = {};
	}

	mod_vasync.forEachParallel({
		inputs: shard_cfg.buckets,
		func: function (bucket_cfg, done) {
			mod_assertplus.string(bucket_cfg.bucket,
				'bucket_cfg.bucket');
			mod_assertplus.number(bucket_cfg.concurrency,
				'bucket_cfg.concurrency');

			var bucket = bucket_cfg.bucket;
			var concurrency = bucket_cfg.concurrency;

			self._ensure_workers_for_bucket(shard, bucket,
				concurrency, done);
		}
	}, ensure_done);
};


/*
 * Internal API method for creating a gc worker. This method may create a new
 * Moray client if the shard for which we're creating a workflow doesn't already
 * exist.
 *
 * This internal API will update all GCManager state to reflect the presence of
 * the new GCWorker and, potentially, moray client.
 */
GCManager.prototype._create_gc_worker = function
_create_gc_worker(shard, bucket, done)
{
	var self = this;

	self.gcm_ctx.ctx_log.debug({
		shard: shard,
		bucket: bucket
	}, 'creating gc worker');

	mod_vasync.pipeline({ funcs: [
		function create_moray_client(_, next) {
			if (self.gcm_ctx.ctx_moray_clients[shard] !==
			    undefined) {
				next();
				return;
			}
			lib_common.create_moray_client(self.gcm_ctx,
				shard, next);
		},
		function create_gc_worker(_, next) {
			var worker_opts = {
				bucket: bucket,
				ctx: self.gcm_ctx,
				log: self.gcm_log,
				shard: shard
			};
			var worker = mod_gc_worker.create_gc_worker(
				worker_opts);

			if (self.gcm_workers[shard] === undefined) {
				self.gcm_workers[shard] = {};
			}
			if (self.gcm_workers[shard][bucket] === undefined) {
				self.gcm_workers[shard][bucket] = [];
			}
			self.gcm_workers[shard][bucket].push(worker);

			next();
		}
	] }, function (err) {
		if (err) {
			self.gcm_log.error(err, 'unable to create gc worker');
		}
		done(err);
	});
};


/*
 * Remove the last worker. We drop the reference to the worker after invoking
 * it's shutdown sequence.
 */
GCManager.prototype._destroy_gc_worker = function
_destroy_gc_worker(shard, bucket, done)
{
	var self = this;
	mod_assertplus.ok(self.gcm_workers[shard], 'self.gcm_workers[shard]');

	if ((self.gcm_workers[shard][bucket] || []).length === 0) {
		done();
		return;
	}

	var worker = self.gcm_workers[shard][bucket].pop();
	worker.shutdown();

	done();
};


GCManager.prototype.pause_gc_workers = function
pause_gc_workers(shards, next)
{
	var self = this;

	mod_vasync.forEachParallel({
		inputs: shards,
		func: function pause_workers(shard, done) {
			Object.keys(self.gcm_workers[shard]).forEach(
				function (bucket) {
				self.gcm_workers[shard][bucket].forEach(
					function (worker) {
					worker.emit('pause');
				});
			});
			done();
		}
	}, next);
};


GCManager.prototype.resume_gc_workers = function
resume_gc_workers(shards, next)
{
	var self = this;

	mod_vasync.forEachParallel({
		inputs: shards,
		func: function resume_workers(shard, done) {
			Object.keys(self.gcm_workers[shard]).forEach(
				function (bucket) {
				self.gcm_workers[shard][bucket].forEach(
					function (worker) {
					worker.emit('resume');
				});
			});
			done();
		}
	}, function (_) {
		next();
	});
};


GCManager.prototype.get_gc_workers = function
get_gc_workers()
{
	var self = this;

	var res = {};

	var shards = Object.keys(self.gcm_workers);

	for (var i = 0; i < shards.length; i++) {
		var shard = shards[i];
		var buckets = Object.keys(self.gcm_workers[shard]);

		if (buckets.length === 0) {
			continue;
		}

		for (var j = 0; j < buckets.length; j++) {
			var bucket = buckets[j];

			if (self.gcm_workers[shard][bucket].length === 0) {
				continue;
			}
			if (res[shard] === undefined) {
				res[shard] = {};
			}

			res[shard][bucket] =
			    self.gcm_workers[shard][bucket].map(
				function (worker) {
				return (worker.describe());
			});
		}
	}

	return (res);
};


function
create_gc_manager(ctx, callback)
{
	ctx.ctx_gc_manager = new GCManager({ ctx: ctx });
	setImmediate(callback);
}


module.exports = {
	create_gc_manager: create_gc_manager
};
