/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

/*
 * Utility functions for interfacing with Moray. Generally these accept a Moray
 * client and a worker.
 */

var mod_assertplus = require('assert-plus');
var mod_fsm = require('mooremachine');
var mod_util = require('util');


var DEFAULT_FINDOBJECTS_FILTER = '(_mtime>=0)';

function
MorayDeleteRecordReader(opts)
{
	var self = this;

	mod_assertplus.object(opts, 'opts');
	mod_assertplus.object(opts.ctx, 'opts.ctx');
	mod_assertplus.string(opts.bucket, 'opts.bucket');
	mod_assertplus.string(opts.shard, 'opts.shard');
	mod_assertplus.object(opts.listener, 'opts.listener');
	mod_assertplus.object(opts.log, 'opts.log');

	self.mr_ctx = opts.ctx;
	self.mr_log = opts.log.child({
		component: 'MorayDeleteRecordReader'
	});
	self.mr_bucket = opts.bucket;
	self.mr_shard = opts.shard;

	/*
	 * Exponential backoff for the error state.
	 */
	self.mr_err_timeout = 1000;
	self.mr_err_ceil = 16000;

	/*
	 * Number of records received via the most recent findObjects RPC
	 * performed.
	 */
	self.mr_prev_records_received = 0;

	/*
	 * Exponential backoff for retries when findObjects returns no records.
	 * This is applied as an additional backoff on top of the operator
	 * defined 'record_read_wait_interval', which is the default wait
	 * interval between non-empty results from findObjects.
	 */
	self.mr_empty_backoff = 0;
	self.mr_empty_backoff_floor = 1000;
	self.mr_empty_backoff_ceil = 16000;
	self.mr_can_reset = false;

	/*
	 * When the delete queue is empty, the garbage collector will repeatedly
	 * print the same log entry indicating that it is pausing before the
	 * next findObjects attempt. In order to avoid cluttering the logs, we
	 * pause after 3 consecutive empty findObjects results.
	 */
	self.mr_allowed_retry_logs = 3;
	self.mr_retry_logs = 0;

	/*
	 * The target to which we pass delete records. This is an EventEmitter
	 * that listens for the 'record' event.
	 */
	self.mr_listener = opts.listener;

	/*
	 * Line of communication between the component that deletes records
	 * from the shard and the reader.  When the record clear removes
	 * rows from the table, we should decrement our offset within it
	 * by the number of rows removed.  In theory, our offset within the
	 * table should be equal to the number of records that we have
	 * cached.  Flushing part (or all) of our cache and removing those
	 * rows from the table is essentially the removal of some (or all) rows
	 * between offset 0 and our current offset.  This means that any new
	 * records that are later added to the table will begin at:
	 *
	 *  (<current offset> - <number of entries removed>)
	 *
	 *  Because we can not cache (and later remove) rows from the table that
	 *  it does not have, it follows that using the above heuristic will
	 *  ensure that our offset never goes below zero.
	 */
	self.mr_moray_listener = opts.listener.mt_moray_listener;

	self.mr_moray_listener.on('delete', function (num_entries) {
		self._decr_offset(num_entries);
	});

	mod_fsm.FSM.call(self, 'running');
}
mod_util.inherits(MorayDeleteRecordReader, mod_fsm.FSM);


/*
 * Wrap all configuration options that can change in getter functions.
 */

MorayDeleteRecordReader.prototype._get_moray_client = function
_get_moray_client()
{
	return (this.mr_ctx.ctx_moray_clients[this.mr_shard]);
};


MorayDeleteRecordReader.prototype._get_tunables_ref = function
_get_tunables_ref()
{
	return (this.mr_ctx.ctx_cfg.tunables);
};


MorayDeleteRecordReader.prototype._get_bucket_ref = function
_get_bucket_ref()
{
	var self = this;
	var buckets = self.mr_ctx.ctx_cfg.buckets;
	var bucket_ref = undefined;

	for (var i = 0; i < buckets.length; i++) {
		var bucket = buckets[i];
		if (bucket.name === self.mr_bucket) {
			bucket_ref = bucket;
		}
	}

	if (bucket_ref.record_read_offset === undefined) {
		bucket_ref.record_read_offset = 0;
	}

	return (bucket_ref);
};


MorayDeleteRecordReader.prototype._get_batch_size = function
_get_batch_size()
{
	return (this._get_tunables_ref().record_read_batch_size);
};


MorayDeleteRecordReader.prototype._get_delay = function
_get_delay()
{
	return (this._get_tunables_ref().record_read_wait_interval);
};


MorayDeleteRecordReader.prototype._get_offset = function
_get_offset()
{
	return (this._get_bucket_ref().record_read_offset);
};


MorayDeleteRecordReader.prototype._incr_offset = function
_incr_offset(delta)
{
	this._get_bucket_ref().record_read_offset += delta;
};


MorayDeleteRecordReader.prototype._decr_offset = function
_decr_offset(delta)
{
	var offset = this._get_bucket_ref().record_read_offset;

	this.mr_log.info('Reducing table offset from %d to %d.',
	    offset, offset - delta);

	this._get_bucket_ref().record_read_offset -= delta;
};


MorayDeleteRecordReader.prototype._get_sort_attr = function
_get_sort_attr()
{
	return (this._get_tunables_ref().record_read_sort_attr);
};


MorayDeleteRecordReader.prototype._get_sort_order = function
_get_sort_order()
{
	return (this._get_tunables_ref().record_read_sort_order);
};


MorayDeleteRecordReader.prototype._is_allowed_creator = function
_is_allowed_creator(creator)
{
	var self = this;

	var creators = self.mr_ctx.ctx_cfg.allowed_creators;

	for (var i = 0; i < creators.length; i++) {
		if (creators[i].uuid === creator) {
			return (true);
		}
	}

	return (false);
};


MorayDeleteRecordReader.prototype._get_collector = function
_get_collector()
{
	var self = this;

	return (self.mr_ctx.ctx_metrics_manager.collector);
};


MorayDeleteRecordReader.prototype._update_empty_backoff = function
_update_empty_backoff()
{
	var self = this;

	if (self.mr_empty_backoff === 0) {
		self.mr_empty_backoff = self.mr_empty_backoff_floor;
		return;
	}

	self.mr_empty_backoff = Math.min(2 * self.mr_empty_backoff,
		self.mr_empty_backoff_ceil);
};


MorayDeleteRecordReader.prototype._reset_empty_backoff = function
_reset_empty_backoff()
{
	var self = this;

	self.mr_empty_backoff = 0;
};


MorayDeleteRecordReader.prototype._find_objects = function
_find_objects() {
	var self = this;
	var moray_client = self._get_moray_client();

	/*
	 * This internal method will never be called without a moray client.
	 * This would have been detected in state_running.
	 */
	mod_assertplus.object(moray_client, 'moray_client');

	var batch = self._get_batch_size();
	var offset = self._get_offset();

	var find_objects_opts = {
		limit: batch,
		offset: offset,
		sort: {
			attribute: self._get_sort_attr(),
			order: self._get_sort_order()
		}
	};

	self.mr_log.info({
		bucket: self.mr_bucket,
		offset: offset,
		limit: batch
	}, 'Calling findobjects.');


	var find_objects_filter = DEFAULT_FINDOBJECTS_FILTER;

	return (moray_client.findObjects(self.mr_bucket,
		find_objects_filter, find_objects_opts));
};


MorayDeleteRecordReader.prototype.state_running = function
state_running(S)
{
	var self = this;

	/*
	 * There must be a Moray client registered for our shard in order to
	 * garbage collect
	 */
	if (!self._get_moray_client()) {
		self.mr_log.error({
			shard: self.mr_shard
		}, 'Reader has no Moray client.');

		setImmediate(function () {
			if (check_cancelled()) {
				return;
			}
			S.gotoState('error');
		});
		return;
	}

	var req = self._find_objects();
	var num_seen = 0;

	var paused_in_flight = false;
	var shutdown_in_flight = false;

	function check_cancelled() {
		if (shutdown_in_flight) {
			S.gotoState('shutdown');
		} else if (paused_in_flight) {
			S.gotoState('paused');
		}
		return (shutdown_in_flight || paused_in_flight);
	}

	req.on('record', function (record) {
		/*
		 * Today, object metadata for objects created with the Manta
		 * multipart upload API do not have creator uuids. For these
		 * objects, we fall back to checking the owner uuid instead.
		 */
		var creator = record.value.creator || record.value.owner;

		/*
		 * Despite the fact that it would be alarming to not have
		 * account information on an object at this stage, log the
		 * event but do not alter our current course of action.
		 */
		if (!creator) {
			self.mr_log.error({record: record},
			    'MorayDeleteRecordReader: missing information');
		}

		if (self._is_allowed_creator(creator)) {
			self.mr_listener.emit('record', record);
			self.mr_log.info({ record: record.key },
			    'Received key.');
			num_seen++;
		}
	});

	req.on('error', function (err) {
		self.mr_log.warn({
			shard: self.mr_shard,
			bucket: self.mr_bucket,
			err: err.message
		}, 'Error encountered while reading Moray delete records. ' +
		'Retrying in %d ms.', self.mr_err_timeout);
		if (check_cancelled()) {
			return;
		}
		S.gotoState('error');
	});

	req.on('end', function () {
		if (num_seen === 0) {
			/*
			 * Back off exponentially each time we consecutively
			 * receive 0 delete records. The queue is not
			 * necessarily empty at this point.
			 */
			if (self.mr_prev_records_received === 0)
				self._update_empty_backoff();
		} else {
			/*
			 * Increment our current offset by the number of
			 * records we just received.
			 */
			self._incr_offset(num_seen);

			self.mr_log.info({
				bucket: self.mr_bucket,
				shard: self.mr_shard,
				num: num_seen
			}, 'Received records.');

			/*
			 * Collector received records.  Reset any exponentiated
			 * backoff that may have accumulated from one or more
			 * consecutive times that the collector saw 0 records
			 * from the Moray client's call to `findObjects()'.
			 */
			self._reset_empty_backoff();

			if (self.mr_ctx.ctx_metrics_manager) {
				self._get_collector().getCollector(
					'gc_delete_records_read').observe(
					num_seen, {
					bucket: self.mr_bucket,
					shard: self.mr_shard
				});
			}
		}

		/*
		 * We completed a findObjects successfully, so reset the error
		 * timeout.
		 */
		self.mr_err_timeout = 1000;

		self.mr_prev_records_received = num_seen;

		if (check_cancelled()) {
			return;
		}
		S.gotoState('waiting');
	});

	S.on(self, 'assertPause', function () {
		self.mr_log.debug('Pausing delete record reader.');
		paused_in_flight = true;
	});
	S.on(self, 'assertResume', function () {
		self.emit('running');
	});
	S.on(self, 'assertShutdown', function () {
		self.mr_log.debug('Shutting down delete record reader.');
		shutdown_in_flight = true;
	});

	self.emit('running');
};


MorayDeleteRecordReader.prototype.state_paused = function
state_paused(S) {
	var self = this;

	S.on(self, 'assertResume', function () {
		S.gotoState('running');
	});
	S.on(self, 'assertPause', function () {
		self.emit('paused');
	});
	S.on(self, 'assertShutdown', function () {
		S.gotoState('shutdown');
	});

	self.emit('paused');
};


MorayDeleteRecordReader.prototype.state_waiting = function
state_waiting(S)
{
	var self = this;

	if (self.mr_prev_records_received === 0 &&
	    self.mr_retry_logs < self.mr_allowed_retry_logs) {
		self.mr_log.debug({
			bucket: self.mr_bucket,
			shard: self.mr_shard,
			offset: self._get_offset(),
			last_received: self.mr_prev_records_received
		}, 'Reader waiting %d milliseconds before ' +
			'next findObjects.', self._get_delay() +
			self.mr_empty_backoff);

		self.mr_retry_logs++;

		if (self.mr_retry_logs === self.mr_allowed_retry_logs) {
			self.mr_log.info('Received no records from %s on ' +
				'%s %d times in a row. Stopping redundant ' +
				'logs.', self.mr_bucket, self.mr_shard,
				self.mr_retry_logs);
		}
	}

	if (self.mr_prev_records_received > 0) {
		self.mr_retry_logs = 0;
	}

	var timer = setTimeout(function () {
		S.gotoState('running');
	}, self._get_delay() + self.mr_empty_backoff);

	S.on(self, 'assertPause', function () {
		clearTimeout(timer);
		S.gotoState('paused');
	});
	S.on(self, 'assertShutdown', function () {
		clearTimeout(timer);
		S.gotoState('shutdown');
	});
};


MorayDeleteRecordReader.prototype.state_error = function
state_error(S)
{
	var self = this;

	self.mr_log.debug({
		shard: self.mr_shard,
		bucket: self.mr_bucket
	}, 'Error while reading delete records, trying again in ' +
		self.mr_err_timeout + ' seconds.');

	var timer = setTimeout(function () {
		self.mr_err_timeout = Math.min(2 * self.mr_err_timeout,
			self.mr_err_ceil);
		S.gotoState('running');
	}, self.mr_err_timeout);

	S.on(self, 'assertResume', function () {
		clearTimeout(timer);
		S.gotoState('running');
	});

	S.on(self, 'assertPause', function () {
		clearTimeout(timer);
		S.gotoState('paused');
	});

	S.on(self, 'assertShutdown', function () {
		clearTimeout(timer);
		S.gotoState('shutdown');
	});
};


MorayDeleteRecordReader.prototype.state_shutdown = function
state_shutdown(S)
{
	var self = this;
	self.emit('shutdown');

	S.on(self, 'assertShutdown', function () {
		self.mr_log.debug('Received shutdown request twice!');
	});
};


MorayDeleteRecordReader.prototype.describe = function
describe()
{
	var self = this;

	var descr = {
		component: 'reader',
		bucket: self.mr_bucket,
		state: self.getState()
	};

	return (descr);
};


module.exports = {
	MorayDeleteRecordReader: MorayDeleteRecordReader
};
