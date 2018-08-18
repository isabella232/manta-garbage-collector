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
var mod_morayfilter = require('moray-filter');
var mod_util = require('util');
var mod_vasync = require('vasync');
var mod_verror = require('verror');


var VE = mod_verror.VError;


function
MorayDeleteRecordCleaner(opts)
{
	var self = this;

	mod_assertplus.object(opts, 'opts');
	mod_assertplus.object(opts.ctx, 'opts.ctx');
	mod_assertplus.object(opts.log, 'opts.log');
	mod_assertplus.string(opts.shard, 'opts.shard');
	mod_assertplus.string(opts.bucket, 'opts.bucket');

	self.mc_ctx = opts.ctx;
	self.mc_shard = opts.shard;
	self.mc_bucket = opts.bucket;
	self.mc_batch_delete_in_progress = false;
	self.mc_periodic_deletes_enabled = false;

	self.mc_key_cache = {};
	self.mc_key_cache_count = 0;
	self.mc_last_delete = Date.now();
	self.mc_last_err = null;

	self.mc_err_timeout = 1000;
	self.mc_err_ceil = 16000;

	self.mc_log = opts.log.child({
		component: 'MorayDeleteRecordCleaner'
	});

	mod_fsm.FSM.call(self, 'init');
}
mod_util.inherits(MorayDeleteRecordCleaner, mod_fsm.FSM);


MorayDeleteRecordCleaner.prototype._get_moray_client = function
_get_moray_client()
{
	var self = this;

	return (self.mc_ctx.ctx_moray_clients[self.mc_shard]);
};


MorayDeleteRecordCleaner.prototype._get_moray_cfg = function
_get_moray_cfg()
{
	var self = this;

	return (self.mc_ctx.ctx_moray_cfgs[self.mc_shard]);
};


MorayDeleteRecordCleaner.prototype._get_batch_size = function
_get_batch_size()
{
	var self = this;

	return (self._get_moray_cfg().record_delete_batch_size);
};


MorayDeleteRecordCleaner.prototype._get_delay = function
_get_delay()
{
	var self = this;

	return (self._get_moray_cfg().record_delete_delay);
};


MorayDeleteRecordCleaner.prototype._get_collector = function
_get_collector()
{
	var self = this;

	return (self.mc_ctx.ctx_metrics_manager.collector);
};


MorayDeleteRecordCleaner.prototype._get_cache_capacity = function
_get_cache_capactiy()
{
	var self = this;
	return (self.mc_ctx.ctx_cfg.capacity);
};


MorayDeleteRecordCleaner.prototype._decr_cache_counts = function
_decr_cache_counts(delta)
{
	var self = this;
	self.mc_key_cache_count -= delta;
	self.mc_ctx.ctx_total_cache_entries -= delta;

	if (self.mc_ctx.ctx_metrics_manager) {
		self._get_collector().getCollector('gc_cache_entries').set(
			self.mc_ctx.ctx_total_cache_entries);
	}
};


MorayDeleteRecordCleaner.prototype._incr_cache_counts = function
_incr_cache_counts()
{
	var self = this;
	self.mc_key_cache_count++;
	self.mc_ctx.ctx_total_cache_entries++;

	if (self.mc_ctx.ctx_metrics_manager) {
		self._get_collector().getCollector('gc_cache_entries').set(
			self.mc_ctx.ctx_total_cache_entries);
	}
};


MorayDeleteRecordCleaner.prototype._get_total_cache_entries = function
_get_total_cache_entries()
{
	var self = this;
	return (self.mc_ctx.ctx_total_cache_entries);
};


MorayDeleteRecordCleaner.prototype._parse_and_handle_batch_error = function
_parse_and_handle_batch_error(err, keys_attempted, callback)
{
	var self = this;

	var server_err = mod_verror.findCauseByName(err, 'FastServerError');
	if (!server_err) {
		callback(err);
		return;
	}
	var req_err = mod_verror.findCauseByName(server_err,
		'FastRequestError');
	if (!req_err) {
		callback(err);
		return;
	}
	var info = mod_verror.info(req_err);
	if (!info) {
		callback(err);
		return;
	}
	/*
	 * If the MorayDeleteRecordCleaner failed a 'batch' request, then either
	 * there is a connectivity issue between Moray and the Manatee primary,
	 * or one of the keys we attempted to delete from the delete queue
	 * doesn't exist. This can happen if two findObjects RPCs with the same
	 * offset and batch size occur between two batch deletes when the
	 * garbage-collector is reading at a faster rate than new keys are
	 * being inserted into that queue.
	 *
	 * Since we cannot control the rate at which keys are being inserted
	 * into the queue, this problem is unavoidable and we must re-read the
	 * records to continue making forward progress.
	 *
	 * The downside here is that there may only be a small subset of keys in
	 * the batch that have already been deleted, in which case we've thrown
	 * out the work done to process the remaining keys that have yet to be
	 * deleted.
	 *
	 * In practice, the described situation will only happen on deployments
	 * with very small delete queues on which deletions are being issued
	 * infrequently.
	 */
	if (info.rpcMethod === 'batch') {
		keys_attempted.forEach(function (key) {
			delete (self.mc_key_cache[key]);
		});
	}

	callback();
};


MorayDeleteRecordCleaner.prototype._batch_delete = function
_batch_delete(done)
{
	var self = this;

	/*
	 * We need to synchronize the periodic batch deleting with new cleanup
	 * requests that may trigger batch deletes. We never want to delete the
	 * same record twice.
	 */
	if (self.mc_batch_delete_in_progress === true) {
		self.mc_log.debug('Batch delete already in progress.');
		setImmediate(done);
		return;
	}

	self.mc_batch_delete_in_progress = true;

	var delete_done = function (err) {
		if (!err) {
			self.mc_last_delete = Date.now();
		}
		self.mc_batch_delete_in_progress = false;
		done(err);
	};

	var keys = Object.keys(self.mc_key_cache);
	var client = self._get_moray_client();

	if (!client) {
		delete_done(new VE('Cleaner missing moray client.'));
		return;
	}

	var reqs = keys.map(function (key) {
		return ({
			operation: 'delete',
			bucket: self.mc_bucket,
			key: key
		});
	});

	if (reqs.length === 0) {
		self.mc_log.debug('Skipping batch delete for 0 records.');
		delete_done();
		return;
	}

	client.batch(reqs, {}, function (err) {
		if (err) {
			self.mc_log.error({
				bucket: self.mc_bucket,
				shard: self.mc_shard,
				err: err.message,
				count: keys.length
			}, 'Error encountered while cleaning delete records.');
			self._parse_and_handle_batch_error(err, keys,
				function (forwarded_err) {
				delete_done(forwarded_err);
			});
			return;
		}

		self.mc_log.info({
			bucket: self.mc_bucket,
			shard: self.mc_shard,
			count: keys.length
		}, 'Cleaned delete records.');

		if (self.mc_ctx.ctx_metrics_manager) {
			self._get_collector().getCollector(
				'gc_delete_records_cleaned').observe(
				keys.length, {
				bucket: self.mc_bucket,
				shard: self.mc_shard
			});
		}

		keys.forEach(function (key) {
			delete (self.mc_key_cache[key]);
		});

		self._decr_cache_counts(keys.length);

		delete_done();
	});
};


MorayDeleteRecordCleaner.prototype._start_periodic_batch_deletes = function
_start_periodic_batch_deletes()
{
	var self = this;

	if (self.mc_periodic_deletes_enabled) {
		return;
	}

	self.mc_periodic_deletes_enabled = true;

	function schedule_tick() {
		setTimeout(tick, self._get_delay());
	}

	function tick_cancelled() {
		return (!self.mc_periodic_deletes_enabled);
	}

	function tick() {
		if (tick_cancelled()) {
			return;
		}
		if (Date.now() - self.mc_last_delete < self._get_delay()) {
			self.mc_log.debug({
				last_delete: self.mc_last_delete,
				delay: self._get_delay()
			}, 'Skipping periodic record cleaning.');
			schedule_tick();
			return;
		}

		if (self.mc_key_cache_count > 0) {
			self._batch_delete(function (err) {
				if (err) {
					self.mc_last_err = err;
					self.emit('assertError');
				}
				schedule_tick();
			});
			return;
		}
		schedule_tick();
	}

	schedule_tick();
};


MorayDeleteRecordCleaner.prototype._stop_periodic_batch_deletes = function
_stop_periodic_batch_deletes()
{
	var self = this;

	self.mc_periodic_deletes_enabled = false;
};


MorayDeleteRecordCleaner.prototype._handle_cleanup_request = function
_handle_cleanup_request(key, done)
{
	var self = this;
	if (self.mc_key_cache.hasOwnProperty(key)) {
		self.mc_log.debug({
			key: key
		}, 'Ignoring duplicate cleanup request.');
		setImmediate(done);
		return;
	}

	if (self._get_total_cache_entries() > self._get_cache_capacity()) {
		self.mc_log.debug({
			used: self._get_total_cache_entries(),
			capacity: self._get_cache_capacity()
		}, 'Garbage collector reached cache capacity. Ignoring ' +
		'delete request.');
		setImmediate(done);
		return;
	}

	self.mc_key_cache[key] = true;
	self._incr_cache_counts();

	if (!self.isInState('error') &&
	    self.mc_key_cache_count >= self._get_batch_size()) {
		self._batch_delete(done);
		return;
	}

	setImmediate(done);
};


MorayDeleteRecordCleaner.prototype.state_init = function
state_init(S)
{
	var self = this;

	S.gotoState('running');

	self.on('cleanup', function (keys) {
		mod_vasync.forEachParallel({
			inputs: Array.isArray(keys) ? keys : [keys],
			func: function (key, done) {
				self._handle_cleanup_request(key, done);
			}
		}, function (err) {
			if (err) {
				self.mc_last_err = err;
				self.emit('assertError');
				return;
			}
			self.mc_err_timeout = 1000;
		});
	});
};


MorayDeleteRecordCleaner.prototype.state_running = function
state_running(S)
{
	var self = this;

	self._start_periodic_batch_deletes();

	S.on(self, 'assertPause', function () {
		S.gotoState('paused');
	});

	S.on(self, 'assertResume', function () {
		self.emit('running');
	});

	S.on(self, 'assertShutdown', function () {
		S.gotoState('shutdown');
	});

	S.on(self, 'assertError', function () {
		S.gotoState('error');
	});

	self.emit('running');
};


MorayDeleteRecordCleaner.prototype.state_paused = function
state_paused(S)
{
	var self = this;

	self._stop_periodic_batch_deletes();

	S.on(self, 'assertResume', function () {
		S.gotoState('running');
	});

	S.on(self, 'assertPause', function () {
		setImmediate(function () {
			self.emit('paused');
		});
	});

	S.on(self, 'assertShutdown', function () {
		S.gotoState('shutdown');
	});

	S.on(self, 'assertError', function () {
		S.gotoState('error');
	});

	self.emit('paused');
};


MorayDeleteRecordCleaner.prototype.state_error = function
state_error(S)
{
	var self = this;

	self._stop_periodic_batch_deletes();

	self.mc_log.info('Cleaner is in error state. Re-running in %d ' +
		'milliseconds.', self.mc_err_timeout);

	var timer = setTimeout(function () {
		self.mc_err_timeout = Math.min(2 * self.mc_err_timeout,
			self.mc_err_ceil);
		S.gotoState('running');
	}, self.mc_err_timeout);

	S.on(self, 'assertResume', function () {
		clearTimeout(timer);
		self.mc_err_timeout = 1000;
		S.gotoState('running');
	});

	S.on(self, 'assertPause', function () {
		clearTimeout(timer);
		self.mc_err_timeout = 1000;
		S.gotoState('paused');
	});

	S.on(self, 'assertShutdown', function () {
		clearTimeout(timer);
		S.gotoState('shutdown');
	});
};


MorayDeleteRecordCleaner.prototype.state_shutdown = function
state_shutdown(S)
{
	var self = this;

	self._stop_periodic_batch_deletes();

	S.on(self, 'assertShutdown', function () {
		self.mc_log.debug('Received shutdown event ' +
			'multiple times!');
	});

	self.emit('shutdown');
};


MorayDeleteRecordCleaner.prototype.describe = function
describe()
{
	var self = this;

	var descr = {
		component: 'cleaner',
		state: self.getState(),
		cached: self.mc_key_cache_count
	};

	return (descr);
};

module.exports = {

	MorayDeleteRecordCleaner: MorayDeleteRecordCleaner

};
