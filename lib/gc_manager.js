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
			/*
			 * If the gc manager initializiation failed, then we
			 * cannot establish a good state and the error likely
			 * requires operator intervention.
			 */
			process.exit(1);
		}
	});
};


GCManager.prototype.state_running = function
state_running(S)
{
	var self = this;

	self.gcm_log.debug('running state');
};


// --- Helpers


GCManager.prototype._shard_num_to_domain = function
_shard_num_to_domain(n)
{
	var self = this;

	var suffix = self.gcm_ctx.ctx_cfg.shards.domain_suffix;

	return ([n, suffix].join('.'));
};


GCManager.prototype._delete_global_shard_ctx_fields = function
_delete_global_shard_ctx_fields(ctx)
{
	delete (ctx.buckets);
	delete (ctx.interval);
	delete (ctx.domain_suffix);
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
		var domain = self._shard_num_to_domain(shard_num);

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

	function create_or_update_ctx(shard_num, done) {
		var domain = self._shard_num_to_domain(shard_num);

		/*
		 * The 'desired' object contains configuration params
		 * that should override the default configuration for
		 * all need
		 */
		self._create_or_update_shard_ctx(domain, desired, done);
	}

	mod_vasync.pipeline({ funcs: [
		function create_workers(_, next) {
			mod_vasync.forEachParallel({
				inputs: updates.create_or_update,
				func: create_or_update_ctx
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
				self.ensure_workers(next);
			});
		},
		function destroy_workers(_, next) {
			mod_vasync.forEachParallel({
				inputs: updates.destroy,
				func: cleanup_workers_and_ctx
			}, next);
		}
	]}, function (err) {
		if (err) {
			callback(err);
			return;
		}
		self._update_global_shards_ctx(desired);
		callback();
	});
};


// -- Context management routines


/*
 * Update logic for fields that are included in both the single shard and
 * global shard configuration.
 */
GCManager.prototype._update_shard_ctx_common = function
_update_shard_ctx_common(curr, updates)
{
	if (updates.buckets === undefined) {
		return;
	}
	if (curr.buckets === undefined) {
		curr.buckets = [];
	}

	updates.buckets.forEach(function (bucket_cfg) {
		var name = bucket_cfg.bucket;

		for (var i = 0; i < curr.buckets.length; i++) {
			if (curr.buckets[i].bucket == name) {
				curr.buckets[i] = mod_jsprim.mergeObjects(
					curr.buckets[i], bucket_cfg);
				return;
			}
		}

		curr.buckets.push(bucket_cfg);
	});
};


/*
 * Update the global configuration describing what shards the garbage collector
 * is processing records from.
 */
GCManager.prototype._update_global_shards_ctx = function
_update_global_shards_ctx(updates)
{
	var self = this;

	self._update_shard_ctx_common(self.gcm_ctx.ctx_cfg.shards, updates);

	if (updates.interval) {
		self.gcm_ctx.ctx_cfg.shards.interval = updates.interval;
	}

	var filtered = mod_jsprim.mergeObjects(updates, null, {});
	self._delete_global_shard_ctx_fields(filtered);

	self.gcm_ctx.ctx_cfg.params.moray = mod_jsprim.mergeObjects(
		self.gcm_ctx.ctx_cfg.params.moray, filtered);
};


/*
 * Update configuration for a single shard.
 */
GCManager.prototype._update_single_shard_ctx = function
_update_single_shard_ctx(shard, updates)
{
	var self = this;

	self._update_shard_ctx_common(self.gcm_ctx.ctx_moray_cfgs[shard],
		updates);

	var filtered = mod_jsprim.mergeObjects(updates, null, {});
	self._delete_global_shard_ctx_fields(filtered);

	self.gcm_ctx.ctx_moray_cfgs[shard] = mod_jsprim.mergeObjects(
		self.gcm_ctx.ctx_moray_cfgs[shard], filtered);
};


/*
 * Initialize the default fields for a new shard context.
 */
GCManager.prototype._create_shard_ctx = function
_create_shard_ctx()
{
	var self = this;
	var defaults = {
		record_read_offset: 0,
		buckets: self.gcm_ctx.ctx_cfg.shards.buckets
	};
	defaults = mod_jsprim.mergeObjects(defaults,
		self.gcm_ctx.ctx_cfg.params.moray);

	return (defaults);
};


/*
 * Update a single-shard configuration if it exists, or create a new
 * configuration and client if they don't already exist.
 */
GCManager.prototype._create_or_update_shard_ctx = function
_create_or_update_shard_ctx(shard, overrides, done)
{
	var self = this;

	if (self.gcm_ctx.ctx_moray_cfgs[shard] !== undefined) {
		self._update_single_shard_ctx(shard, overrides);
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

		var filtered = mod_jsprim.mergeObjects(overrides, null, {});
		self._delete_global_shard_ctx_fields(filtered);
		filtered.buckets = overrides.buckets;

		self.gcm_ctx.ctx_moray_cfgs[shard] = mod_jsprim.mergeObjects(
			self._create_shard_ctx(), filtered);
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
GCManager.prototype.ensure_workers = function
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
	}, 'Setting up desired set of gc workers.');

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
				self.gcm_log.error({
					shard: shard,
					bucket: bucket,
					err: err
				}, 'Error setting up desired workers.');
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
	}, 'Creating gc worker.');

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
			var barrier = mod_vasync.barrier();
			var values = Object.keys(self.gcm_workers[shard]).map(
				function (bucket) {
				return (self.gcm_workers[shard][bucket]);
			});
			var workers = [].concat.apply([], values);
			if (workers.length === 0) {
				done();
				return;
			}

			workers.forEach(function (worker, i) {
				var op = 'pause_' + i;
				barrier.start(op);
				workers[i].once('paused', function () {
					barrier.done(op);
				});
				workers[i].pause();
			});

			barrier.once('drain', done);
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
			var barrier = mod_vasync.barrier();
			var values = Object.keys(self.gcm_workers[shard]).map(
				function (bucket) {
				return (self.gcm_workers[shard][bucket]);
			});
			var workers = [].concat.apply([], values);

			if (workers.length === 0) {
				done();
				return;
			}

			workers.forEach(function (worker, i) {
				var op = 'resume_' + i;
				barrier.start(op);
				workers[i].once('running', function () {
					barrier.done(op);
				});
				workers[i].resume();
			});

			barrier.once('drain', done);
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


GCManager.prototype.get_total_cache_entries = function
get_total_cache_entries()
{
	var self = this;

	return (self.gcm_ctx.total_cache_entries);
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
