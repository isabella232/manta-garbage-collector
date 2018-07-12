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
	self.mr_moray_bucket = opts.bucket;
	self.mr_shard = opts.shard;

	/*
	 * Exponential backoff for the error state.
	 */
	self.mr_err_timeout = 1000;
	self.mr_err_ceil = 16000;

	/*
	 * The target to which we pass delete records. This is an EventEmitter
	 * that listens for the 'record' event.
	 */
	self.mr_listener = opts.listener;

	mod_fsm.FSM.call(self, 'running');
};
mod_util.inherits(MorayDeleteRecordReader, mod_fsm.FSM);


/*
 * Wrap all configuration options that can change in getter functions.
 */

MorayDeleteRecordReader.prototype._get_moray_client = function
_get_moray_client()
{
	var self = this;
	return self.mr_ctx.ctx_moray_clients[self.mr_shard];
};


MorayDeleteRecordReader.prototype._get_moray_cfg = function
_get_moray_cfg()
{
	var self = this;
	return self.mr_ctx.ctx_moray_cfgs[self.mr_shard];
};


MorayDeleteRecordReader.prototype._get_record_read_wait_interval = function
_get_record_read_wait_interval()
{
	var self = this;
	return self._get_moray_cfg().record_read_wait_interval;
};


MorayDeleteRecordReader.prototype._get_record_read_batch_size = function
_get_record_read_batch_size()
{
	var self = this;

	return self._get_moray_cfg().record_read_batch_size;
};


MorayDeleteRecordReader.prototype._get_and_set_record_read_offset = function
_get_and_set_record_read_offset(delta)
{
	var self = this;

	var offset = self._get_moray_cfg().record_read_offset;
	self._get_moray_cfg().record_read_offset += delta;

	return offset;
};


MorayDeleteRecordReader.prototype._reset_record_read_offset = function
_reset_record_read_offset()
{
	var self = this;

	self._get_moray_cfg().record_read_offset = 0;
};


MorayDeleteRecordReader.prototype._get_record_read_sort_attr = function
_get_record_read_sort_attr()
{
	var self = this;

	return self._get_moray_cfg().record_reader_sort_attr;
};


MorayDeleteRecordReader.prototype._get_record_read_sort_order = function
_get_record_read_sort_order()
{
	var self = this;

	return self._get_moray_cfg().record_read_sort_order;
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

	var batch = self._get_record_read_batch_size();
	var offset = self._get_and_set_record_read_offset(batch);

	var find_objects_opts = {
		limit: batch,
		offset: offset,
		sort: {
			attribute: self._get_record_read_sort_attr(),
			order: self._get_record_read_sort_order()
		}
	};

	self.mr_log.debug({
		bucket: self.mr_moray_bucket,
		offset: offset,
		limit: batch,
		opts: find_objects_opts
	}, 'findobjects');


	var find_objects_filter = DEFAULT_FINDOBJECTS_FILTER;

	return (moray_client.findObjects(self.mr_moray_bucket, find_objects_filter,
	    find_objects_opts));
};


MorayDeleteRecordReader.prototype._get_collector = function
_get_collector()
{
	var self = this;

	return self.mr_ctx.ctx_metrics_manager.collector;
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
		self.mr_log.warn({
			shard: self.mr_shard
		}, 'no moray client found');
		S.gotoState('error');
		return;
	}

	var req = self._find_objects();
	var records_received = 0;
	var paused_in_flight = false;
	var shutdown_in_flight = false;

	function check_cancelled() {
		if (shutdown_in_flight) {
			S.gotoState('shutdown');
		} else if (paused_in_flight) {
			S.gotoState('paused');
		}
		return shutdown_in_flight || paused_in_flight;
	}

	req.on('record', function (record) {
		self.mr_log.debug({
			record: mod_util.inspect(record)
		}, 'received record');
		self.mr_listener.emit('record', record);
		records_received++;
	});

	req.on('error', function (err) {
		self.mr_log.error({
			shard: self.mr_shard,
			bucket: self.mr_moray_bucket,
			err: err
		}, 'error reading Moray delete records');
		if (check_cancelled()) {
			return;
		}
		S.gotoState('error');
	});

	req.on('end', function () {
		self.mc_err_timeout = 1000;
		if (records_received === 0) {
			self.mr_log.debug('exhausted delete record queue');
			self._reset_record_read_offset();
		} else {
			self.mr_log.debug('received %s records', records_received);

			self._get_collector().getCollector('delete_records_read').observe(
				records_received, {
				bucket: self.mr_moray_bucket,
				shard: self.mr_shard
			});
		}

		if (check_cancelled()) {
			return;
		}
		S.gotoState('waiting');
	});

	S.on(self, 'pause', function() {
		self.mr_log.debug('pausing delete record reader');
		paused_in_flight = true;
	});
	S.on(self, 'shutdown', function() {
		self.mr_log.debug('shutting down delete record reader');
		shutdown_in_flight = true;
	});
};


MorayDeleteRecordReader.prototype.state_paused = function
state_paused(S) {
	var self = this;

	self.mr_log.info({
		shard: self.mr_shard,
		bucket: self.mr_bucket
	}, 'record reader paused');

	S.on(self, 'resume', function () {
		S.gotoState('running');
	});
	S.on(self, 'shutdown', function () {
		S.gotoState('shutdown');
	});
};


MorayDeleteRecordReader.prototype.state_waiting = function
state_waiting(S)
{
	var self = this;

	var timer = setTimeout(function () {
		S.gotoState('running');
	}, self._get_record_read_wait_interval());

	S.on(self, 'pause', function() {
		clearTimeout(timer);
		S.gotoState('paused');
	});
	S.on(self, 'shutdown', function() {
		clearTimeout(timer);
		S.gotoState('shutdown');
	});
};


MorayDeleteRecordReader.prototype.state_error = function
state_error(S)
{
	var self = this;

	self.mr_log.info({
		shard: self.mr_shard,
		bucket: self.mr_bucket
	}, 'unable to read delete records, trying again in ' +
		self.mr_err_timeout + ' seconds');

	/*
	 * No matter what the error condition is, we always start reading from
	 * the beginning of the delete record queue in order to prevent some
	 * records from hanging around in that table for a really long time.
	 */
	self.mr_offset = 0;

	var timer = setTimeout(function () {
		self.mr_err_timeout = Math.min(2 * self.mr_err_timeout,
			self.mr_err_ceil);
		S.gotoState('running');
	}, self.mr_err_timeout);

	S.on(self, 'resume', function () {
		clearTimeout(timer);
		S.gotoState('running');
	});

	S.on(self, 'pause', function () {
		clearTimeout(timer);
		S.gotoState('paused');
	});

	S.on(self, 'shutdown', function() {
		clearTimeout(timer);
		S.gotoState('shutdown');
	});
};


MorayDeleteRecordReader.prototype.state_shutdown = function
state_shutdown(S)
{
	var self = this;
	self.mr_log.debug('shutdown');
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
}
