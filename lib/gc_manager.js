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
var mod_jsprim = require('jsprim');
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
	self.gcm_workers = {};

	mod_fsm.FSM.call(this, 'init');
}
mod_util.inherits(GCManager, mod_fsm.FSM);


GCManager.prototype.state_init = function
state_init(S)
{
	var self = this;

	self.setup_shards(self.gcm_ctx.ctx_cfg.shards, function (err) {
		if (err) {
			self.gcm_log.error(err, 'unable to init ' +
				'gc manager');
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
 * This function accepts a configuration object describing
 * the desired set of shards and what buckets on each of those
 * shards this garbage collector process should poll.
 */
GCManager.prototype.setup_shards = function
setup_shards(desired, callback)
{
	var self = this;

	var shards_cfg = self.gcm_ctx.ctx_cfg.shards;
	var domain_suffix = shards_cfg.domain_suffix;

	var curr_interval = shards_cfg.interval;
	var next_interval = desired.interval || curr_interval;

	var updates = {
		create_or_update: [],
		destroy: []
	};

	for (var i = next_interval[0]; i <= next_interval[1]; i++) {
		updates.create_or_update.push(i);
	}

	for (i = curr_interval[0]; i <= curr_interval[1]; i++) {
		if (i < next_interval[0] || i > next_interval[1]) {
			updates.destroy.push(i);
		}
	}

	function cleanup_workers_and_ctx(shard_num, done) {
		var domain = [shard_num, domain_suffix].join('.');

		mod_vasync.pipeline({ funcs: [
			function (_, next) {
				self._destroy_workers_for_shard(domain, next);
			},
			function (_, next) {
				self._cleanup_shard_ctx(domain);
				next();
			}
		]}, done);
	}

	function update_ctx(shard_num, done) {
		var domain = [shard_num, domain_suffix].join('.');

		/*
		 * The 'desired' object contains configuration params
		 * that should override the default configuration for
		 * all need
		 */
		self._create_or_update_shard_ctx(domain, desired, done);
	}

	mod_vasync.pipeline({ funcs: [
		function destroy_workers(_, next) {
			mod_vasync.forEachParallel({
				inputs: updates.destroy,
				func: cleanup_workers_and_ctx
			}, next);
		},
		function create_workers(_, next) {
			mod_vasync.forEachParallel({
				inputs: updates.create_or_update,
				func: update_ctx
			}, function (err) {
				if (err) {
					next(err);
					return;
				}
				/*
				 * After setting up all the necessary
				 * configuration and clients above, actually
				 * create the gc workers.
				 */
				self._ensure_workers(next);
			});
		}
	]}, function (err) {
		callback(err);
	});
};


// -- Shard state management


/*
 * Creates all state needed to GC from a particular shard.
 */
GCManager.prototype._create_or_update_shard_ctx = function
_create_or_update_shard_ctx(shard, overrides, done)
{
	var self = this;
	var ctx_cfg = self.gcm_ctx.ctx_cfg;
	var shard_cfg = self.gcm_ctx.ctx_moray_cfgs[shard];

	if (self.gcm_ctx.ctx_moray_cfgs[shard] !== undefined) {
		self.gcm_ctx.ctx_moray_cfgs[shard] = mod_jsprim.mergeObjects(
			shard_cfg, overrides);
		setImmediate(done);
		return;
	}

	var defaults = {
		record_read_offset: 0,
		buckets: ctx_cfg.shards.buckets
	};
	defaults = mod_jsprim.mergeObjects(defaults, ctx_cfg.params.moray);

	if (self.gcm_ctx.ctx_moray_clients[shard] !== undefined) {
		self.gcm_ctx.ctx_moray_cfgs[shard] = mod_jsprim.mergeObjects(
			defaults, overrides);
		setImmediate(done);
		return;
	}

	lib_common.create_moray_client(self.gcm_ctx, shard,
		function (err, client) {
		if (err) {
			done(err);
			return;
		}
		self.gcm_ctx.ctx_moray_clients[shard] = client;
		self.gcm_ctx.ctx_moray_cfgs[shard] = mod_jsprim.mergeObjects(
			defaults, overrides);
		done();
	});
};


/*
 * Cleanup all state associated with a shard.
 */
GCManager.prototype._cleanup_shard_ctx = function
_cleanup_shard_ctx(shard)
{
	var self = this;

	self.gcm_ctx.ctx_moray_clients[shard].close();

	delete (self.gcm_ctx.ctx_moray_clients[shard]);
	delete (self.gcm_ctx.ctx_moray_cfgs[shard]);
};


// --- Worker management


/*
 * Ensure that for each shard, we have created the appropriate number of
 * GCWorkers.
 */
GCManager.prototype._ensure_workers = function
ensure_workers(done)
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
			var bucket = bucket_cfg.bucket;
			var concurrency = bucket_cfg.concurrency;

			self._ensure_workers_for_bucket(shard, bucket,
				concurrency, done);
		}
	}, ensure_done);
};


GCManager.prototype._destroy_workers_for_shard = function
_destroy_workers_for_shard(shard, destroy_done)
{
	var self = this;

	mod_vasync.forEachParallel({
		inputs: Object.keys(self.gcm_workers[shard]),
		func: function destroy(bucket, next) {
			self._destroy_worker(shard, bucket, next);
		}
	}, destroy_done);
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
				self._create_worker(shard, bucket, next);
			} else {
				self._destroy_worker(shard, bucket, next);
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


/*
 * Internal API method for creating a gc worker. This method may create a new
 * Moray client if the shard for which we're creating a workflow doesn't already
 * exist.
 *
 * This internal API will update all GCManager state to reflect the presence of
 * the new GCWorker and, potentially, moray client.
 */
GCManager.prototype._create_worker = function
_create_worker(shard, bucket, done)
{
	var self = this;

	self.gcm_ctx.ctx_log.debug({
		shard: shard,
		bucket: bucket
	}, 'creating gc worker');

	var worker_opts = {
		bucket: bucket,
		ctx: self.gcm_ctx,
		log: self.gcm_log,
		shard: shard
	};
	var worker = mod_gc_worker.create_gc_worker(worker_opts);

	if (self.gcm_workers[shard] === undefined) {
		self.gcm_workers[shard] = {};
	}
	if (self.gcm_workers[shard][bucket] === undefined) {
		self.gcm_workers[shard][bucket] = [];
	}
	self.gcm_workers[shard][bucket].push(worker);

	done();
};


/*
 * Remove the last worker. We drop the reference to the worker after invoking
 * it's shutdown sequence.
 */
GCManager.prototype._destroy_worker = function
_destroy_worker(shard, bucket, done)
{
	var self = this;
	mod_assertplus.ok(self.gcm_workers[shard], 'self.gcm_workers[shard]');

	if ((self.gcm_workers[shard][bucket] || []).length === 0) {
		done();
		return;
	}

	var worker = self.gcm_workers[shard][bucket].pop();
	worker.once('shutdown', function () {
		done();
	});

	worker.shutdown();
};


// --- Worker control (pause, resume, list)


GCManager.prototype.pause_workers = function
pause_workers(shards, next)
{
	var self = this;

	mod_vasync.forEachParallel({
		inputs: shards,
		func: function pause(shard, done) {
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


GCManager.prototype.resume_workers = function
resume_workers(shards, next)
{
	var self = this;

	mod_vasync.forEachParallel({
		inputs: shards,
		func: function resume(shard, done) {
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


GCManager.prototype.get_workers = function
get_workers()
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
