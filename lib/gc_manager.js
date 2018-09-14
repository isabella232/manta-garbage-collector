/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2019, Joyent, Inc.
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
	self.gcm_workers = {};

	mod_fsm.FSM.call(this, 'init');
}
mod_util.inherits(GCManager, mod_fsm.FSM);


GCManager.prototype.state_init = function
state_init(S)
{
	var self = this;

	self._create_clients_and_workers(function (err) {
		if (err) {
			self.gcm_log.error(err, 'Unable to initialize ' +
				'GC manager.');
			/*
			 * If the gc manager initializiation failed, then we
			 * cannot establish a good state and the error likely
			 * requires operator intervention.
			 */
			process.exit(1);
		}
		S.gotoState('running');
	});
};


GCManager.prototype.state_running = function
state_running(S)
{
	var self = this;

	self.emit('running');

	self.gcm_log.info('GC Manager running.');
};


// --- Helpers


GCManager.prototype._create_clients_and_workers = function
_create_clients_and_workers(done)
{
	var self = this;

	mod_vasync.forEachParallel({
		inputs: self.gcm_ctx.ctx_cfg.shards,
		func: function setup(shard, finished) {
			self._create_moray_client(shard.host,
				finished);
		}
	}, function (err) {
		if (err) {
			done(err);
			return;
		}
		self._ensure_workers(done);
	});
};


GCManager.prototype._create_moray_client = function
_create_moray_client(shard, done)
{
	var self = this;

	lib_common.create_moray_client(self.gcm_ctx, shard,
		function (err, client) {
		if (err) {
			done(err);
			return;
		}
		self.gcm_ctx.ctx_moray_clients[shard] = client;

		self.gcm_log.info({
			shard: shard
		}, 'Created new moray client.');

		done();
	});
};


// --- Worker management


/*
 * Ensure that for each shard, we have created the appropriate number of
 * GCWorkers.
 */
GCManager.prototype._ensure_workers = function
_ensure_workers(done)
{
	var self = this;

	mod_vasync.forEachParallel({
		inputs: self.gcm_ctx.ctx_cfg.shards,
		func: function (shard, cb) {
			self._ensure_workers_for_shard(shard.host, cb);
		}
	}, function (err) {
		if (err) {
			self.gcm_log.warn({
				err: err
			}, 'Error while ensuring workers.');
		}
		done(err);
	});
};


GCManager.prototype._ensure_workers_for_shard = function
_ensure_workers_for_shard(shard, ensure_done)
{
	var self = this;
	var ctx = self.gcm_ctx;

	if (!self.gcm_workers[shard]) {
		self.gcm_workers[shard] = {};
	}

	mod_vasync.forEachParallel({
		inputs: ctx.ctx_cfg.buckets,
		func: function (bucket, done) {
			var bucket_name = bucket.name;
			var concurrency = ctx.ctx_cfg.concurrency;

			self._ensure_workers_for_bucket(shard,
				bucket_name, concurrency, done);
		}
	}, ensure_done);
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
			return (concurrency >
			    (self.gcm_workers[shard][bucket] ||
			    []).length);
		},
		function create_workers(next) {
			self._create_worker(shard, bucket, next);
		},
		function whilst_done(err) {
			if (err) {
				self.gcm_log.error({
					shard: shard,
					bucket: bucket,
					err: err
				}, 'Error creating workers.');
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
