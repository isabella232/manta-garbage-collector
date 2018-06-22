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
	self.gcm_log = self.gcm_ctx.ctx_log.child({
		component: 'GCManager'
	});
	self.gcm_http_server = opts.ctx.ctx_http_server;

	/*
	 * A mapping from shard srvDomain to array of GCWorkers that process delete
	 * records on that shard:
	 * {
	 * 	'2.moray.orbit.example.com': [...]
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

	self._resolve_gc_workers(function (err) {
		if (err) {
			self.gcm_log(err, 'unable to resolve all workers, retrying...');
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

	self.gcm_log('running state');
};


/*
 * Each GCWorker corresponds to a given shard and, therefore, node-moray client.
 * The per-shard GC configuration is stored in a map in the context object.
 * Hence, the GCManager manages two pieces of state:
 *
 * 1. The set of moray configurations
 * 2. The set of GCWorkers
 *
 * This method resolves these two pieces of state. That is, we ensure that for
 * each shard, we have created the appropriate number of GCWorkers.
 */
GCManager.prototype._resolve_gc_workers = function
_resolve_gc_workers(done)
{
	var self = this;

	mod_vasync.forEachParallel({
		inputs: Object.keys(self.gcm_ctx.ctx_moray_cfgs),
		func: function (shard, cb) {
			self._resolve_workers_for_shard(shard, cb);
		}
	}, function (err) {
		if (err) {
			self.gcm_log.warn({
				err: err
			}, 'error resolving workers');
		}
	});
};


GCManager.prototype._resolve_workers_for_shard = function
_resolve_workers_for_shard(shard, done)
{
	var self = this;

	var shard_cfg = self.gcm_ctx.ctx_moray_cfgs[shard];

	mod_vasync.whilst(
		function unresolved() {
			return (shard_cfg.concurrency !==
				(self.gcm_workers[shard] || []).length);
		},
		function create_or_destroy_worker(next) {
			if ((self.gcm_workers[shard] || []).length <
				shard_cfg.concurrency) {
				self._create_gc_worker(shard, next);
			} else {
				self._destroy_gc_worker(shard, next);
			}
		},
		function whilst_done(err) {
			if (err) {
				self.gcm_log.warn({
					shard: shard,
					err: err
				}, 'error while resolving workers');
			}
			done(err);
		}
	);
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
_create_gc_worker(shard, done)
{
	var self = this;
	mod_vasync.pipeline({ funcs: [
		function create_moray_client(_, next) {
			if (self.gcm_ctx.ctx_moray_clients[shard] !== undefined) {
				next();
				return;
			}
			lib_common.create_moray_client(self.gcm_ctx, shard, next);
		},
		function create_gc_worker(_, next) {
			var worker_opts = {
				ctx: self.gcm_ctx,
				log: self.gcm_log,
				shard: shard,
			};
			if (self.gcm_workers[shard] === undefined) {
				self.gcm_workers[shard] = [];
			}
			self.gcm_workers[shard].push(mod_gc_worker.create_gc_worker(
					worker_opts));
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
_destroy_gc_worker(shard, done)
{
	var self = this;

	if (self.gcm_workers[shard].length === 0) {
		done();
		return;
	}

	var worker = self.gcm_workers[shard].pop();

	worker.shutdown();
};

/*
 * Create many GCWorkers. This method possibly returns a MultiError if it was
 * unable to create some workers.
 */
GCManager.prototype.create_gc_workers = function
create_gc_workers(shards, done)
{
	var self = this;
	var num_created = 0;
	var errors = [];

	shards.forEach(function (shard) {
		self._create_gc_worker(shard, function (err) {
			if (err) {
				errors.push(err);
			} else {
				num_created++;
			}
			if ((num_create + errors.length) === shards.length) {
				if (errors.length > 0) {
					done(new ME(errors));
					return;
				}
				done();
			}
		});
	});
};



GCManager.prototype.pause_gc_workers = function
pause_gc_workers()
{
};


GCManager.prototype.resume_gc_workers = function
resume_gc_workers()
{
};


GCManager.prototype.list_gc_workers = function
list_gc_workers()
{
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
