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
	self.mc_cache_capacity = 10000;

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

	return self.mc_ctx.ctx_moray_clients[self.mc_shard];
};


MorayDeleteRecordCleaner.prototype._get_moray_cfg = function
_get_moray_cfg()
{
	var self = this;

	return self.mc_ctx.ctx_moray_cfgs[self.mc_shard];
};


MorayDeleteRecordCleaner.prototype._get_batch_size = function
_get_batch_size()
{
	var self = this;

	return self._get_moray_cfg().record_delete_batch_size;
};


MorayDeleteRecordCleaner.prototype._get_delay = function
_get_delay()
{
	var self = this;

	return self._get_moray_cfg().record_delete_delay;
};


MorayDeleteRecordCleaner.prototype._construct_request_batch = function
_construct_request_batch(keys)
{
	var self = this;

	return keys.map(function (key) {
		var req = {
			bucket: self.mc_bucket,
	       		operation: 'delete',
	       		key: key
		};

		return req;
	});
};


MorayDeleteRecordCleaner.prototype._construct_moray_filter = function
_construct_moray_filter(keys)
{
	var filter = new mod_morayfilter.OrFilter();

	for (var i = 0; i < keys.length; i++) {
		filter.addFilter(new mod_morayfilter.EqualityFilter({
			attribute: '_key',
			value: keys[i]
		}));
	}

	return (filter.toString());
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
		self.mc_log.debug('batch delete already in progress');
		setImmediate(done);
		return;
	}

	self.mc_batch_delete_in_progress = true;

	var delete_done = function (err) {
		self.mc_batch_delete_in_progress = false;
		done(err);
	};

	var keys = Object.keys(self.mc_key_cache);

	var client = self._get_moray_client();
	var filter = self._construct_moray_filter(keys);

	self.mc_log.debug({
		bucket: self.mc_bucket,
		filter: filter,
		numreqs: keys.length
	}, 'issuing deleteMany');

	client.deleteMany(self.mc_bucket, filter, function (err) {
		if (err) {
			self.mc_log.debug({
				reqs: mod_util.inspect(keys),
				err: err
			}, 'deleteMany error');
			delete_done(err);
			return;
		}

		self.mc_log.debug({
			keys: mod_util.inspect(keys),
			count: keys.length
		}, 'deleteMany done');

		mod_vasync.forEachPipeline({
			inputs: keys,
			func: function (key, next) {
				delete (self.mc_key_cache[key]);
				next();
			}
		}, delete_done);
	});
};


/*
 * Unused. Morays batch feature requires that each of the requests succeed in
 * order for the transaction to commit. Since we can receive multiple cleanup
 * requests for the same key (if a delete record points to two storage nodes --
 * see MakoInstructionUploader), this will not work in the setting.
 */
MorayDeleteRecordCleaner.prototype._alt_batch_delete = function
_alt_batch_delete(done)
{
	var self = this;

	/*
	 * We need to synchronize the periodic batch deleting with new cleanup
	 * requests that may trigger batch deletes. We never want to delete the
	 * same record twice.
	 */
	if (self.mc_batch_delete_in_progress === true) {
		self.mc_log.debug('batch delete already in progress');
		setImmediate(done);
		return;
	}

	self.mc_batch_delete_in_progress = true;

	var delete_done = function (err) {
		self.mc_batch_delete_in_progress = false;
		done(err);
	};

	var keys = Object.keys(self.mc_key_cache);

	var client = self._get_moray_client();
	var reqs = self._construct_request_batch(keys);

	self.mc_log.debug({
		reqs: mod_util.inspect(reqs),
		numreqs: reqs.length
	}, 'issuing batch delete');

	client.batch(reqs, {}, function (err) {
		if (err) {
			self.mc_log.debug({
				reqs: mod_util.inspect(reqs),
				err: err
			}, 'batch delete error');
			delete_done(err);
			return;
		}

		self.mc_log.debug({
			reqs: mod_util.inspect(reqs),
			numreqs: reqs.length
		}, 'finished batch delete');

		mod_vasync.forEachPipeline({
			inputs: keys,
			func: function (key, next) {
				delete (self.mc_key_cache[key]);
				next();
			}
		}, delete_done);
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

	/*
	 * In addition to running a Moray deleteMany when our cache hits a
	 * configurable size, we also periodically flush cleanup requests to
	 * ensure that individual requests under the batch size don't wait
	 * too long to be cleaned up.
	 */
	function tick_flush_cleanup_requests() {
		if (!self.mc_periodic_deletes_enabled) {
			return;
		}
		if (Object.keys(self.mc_key_cache).length > 0) {
			self._batch_delete(function (err) {
				if (err) {
					self.mc_log.warn({
						err: err
					}, 'unable to flush period cleanup requests')
				}
				setTimeout(tick_flush_cleanup_requests, self._get_delay());
			});
			return;
		}
		setTimeout(tick_flush_cleanup_requests, self._get_delay());
	}
	setImmediate(tick_flush_cleanup_requests);
};


MorayDeleteRecordCleaner.prototype._stop_periodic_batch_deletes = function
_stop_periodic_batch_deletes()
{
	var self = this;

	if (!self.mc_periodic_deletes_enabled) {
		return;
	}

	self.mc_periodic_deletes_enabled = false;
};


MorayDeleteRecordCleaner.prototype._cache_cleanup_request = function
_cache_cleanup_request(key, done)
{
	var self = this;
	if (self.mc_key_cache.hasOwnProperty(key)) {
		self.mc_log.warn({
			key: key
		}, 'received duplicated cleanup request');
		done();
		return;
	}

	self.mc_log.debug({
		cache_size: Object.keys(self.mc_key_cache).length,
		delete_batch_size: self._get_batch_size(),
		key: key
	}, 'received cleanup request');

	/*
	 * If we are in the error state, then there are no batch deletes being
	 * issued.
	 */
	if (self.isInState('error') && Object.keys(self.mc_key_cache).length >=
		self.mc_cache_capacity) {
		done();
		return;
	}

	self.mc_key_cache[key] = true;

	/*
	 * Do not attempt to delete records if we are in a bad state, simply
	 * cache them.
	 */
	if (!self.isInState('error') && Object.keys(self.mc_key_cache).length >=
		self._get_batch_size()) {
		self._batch_delete(done);
		return;
	}
	done();
};


MorayDeleteRecordCleaner.prototype.state_init = function
state_init(S)
{
	var self = this;

	self.on('cleanup', function (keys) {
		self.mc_log.debug({
			count: keys.length
		}, 'received batched cleanup request');

		mod_vasync.forEachParallel({
			inputs: Array.isArray(keys) ? keys : [keys],
			func: function (key, done) {
				self._cache_cleanup_request(key, done);
			}
		}, function (err) {
			if (err) {
				self.mc_log.debug({
					count: keys.length,
					err: err
				}, 'error processing cleanup request');
				S.gotoState('error');
			} else {
				self.mc_err_timeout = 1000;
				self.mc_log.debug({
					count: keys.length
				}, 'successfully processed cleanup request');
			}
		});
	});

	S.gotoState('running');
};


MorayDeleteRecordCleaner.prototype.state_running = function
state_running(S)
{
	var self = this;

	self._start_periodic_batch_deletes();

	S.on(self, 'pause', function () {
		self.mc_log.debug('pausing moray delete record cleaner');
		S.gotoState('paused');
	});
};


MorayDeleteRecordCleaner.prototype.state_paused = function
state_paused(S)
{
	var self = this;

	self._stop_periodic_batch_deletes();

	S.on(self, 'resume', function () {
		S.gotoState('running');
	});
};


MorayDeleteRecordCleaner.prototype.state_error = function
state_error(S)
{
	var self = this;

	self._stop_periodic_batch_deletes();

	var timer = setTimeout(function () {
		self.mc_err_timeout = Math.min(2 * self.mc_err_timeout,
			self.mc_err_ceil);
		S.gotoState('running');
	}, self.mc_err_timeout);

	S.on(self, 'resume', function () {
		clearTimeout(timer);
		self.mc_err_timeout = 1000;
		S.gotoState('running');
	});

	S.on(self, 'pause', function () {
		clearTimeout(timer);
		self.mc_err_timeout = 1000;
		S.gotoState('paused');
	});
};


MorayDeleteRecordCleaner.prototype.describe = function
describe()
{
	var self = this;

	var descr = {
		component: 'cleaner',
		state: self.getState()
	};

	return (descr);
};

module.exports = {

	MorayDeleteRecordCleaner: MorayDeleteRecordCleaner

};
