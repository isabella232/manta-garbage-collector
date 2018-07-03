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
	self.mr_offset = 0;

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

	var find_objects_opts = {
		limit: self._get_record_read_batch_size(),
		offset: self.mr_offset,
		sort: {
			attribute: self._get_record_read_sort_attr(),
			order: self._get_record_read_sort_order()
		}
	};

	self.mr_log.debug({
		bucket: self.mr_moray_bucket,
		filter: find_objects_filter,
		opts: find_objects_opts
	}, 'doing findobjects');


	var find_objects_filter = DEFAULT_FINDOBJECTS_FILTER;

	return (moray_client.findObjects(self.mr_moray_bucket, find_objects_filter,
	    find_objects_opts));
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
		S.gotoState('waiting');
		return;
	}

	var req = self._find_objects();
	var records_received = 0;

	req.on('record', function (record) {
		self.mr_log.debug({
			record: mod_util.inspect(record)
		}, 'received record');
		self.mr_listener.emit('record', record);
		records_received++;
	});

	req.on('error', function (err) {
		self.mr_log.error('error reading Moray delete records "%s"', err);
		S.gotoState('error');
	});

	req.on('end', function () {
		if (records_received === 0) {
			self.mr_log.debug('exhausted delete record queue');
			self.mr_offset = 0;
		} else {
			self.mr_log.debug('received %s records', records_received);
			self.mr_offset += records_received;
		}

		S.gotoState('waiting');
	});

	S.on(self, 'pause', function() {
		S.gotoState('paused');
	});
};


MorayDeleteRecordReader.prototype.state_paused = function
state_paused(S) {
	var self = this;

	S.on(self, 'resume', function () {
		S.gotoState('running');
	});
};


MorayDeleteRecordReader.prototype.state_waiting = function
state_waiting(S)
{
	var self = this;

	var timer = setTimeout(function () {
		S.gotoState('running');
	}, self._get_record_read_wait_interval());

	S.on(self, 'pause', function () {
		clearTimeout(timer);
		S.gotoState('paused');
	});
};


MorayDeleteRecordReader.prototype.state_error = function
state_error(S)
{
	var self = this;

	S.on(self, 'resume', function () {
		S.gotoState('running');
	});

	S.on(self, 'pause', function () {
		S.gotoState('paused');
	});
};


module.exports = {
	MorayDeleteRecordReader: MorayDeleteRecordReader
}
