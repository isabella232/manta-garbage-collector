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
	 * The target to which we pass delete records. This is an EventEmitter
	 * that listens for the 'record' event.
	 */
	self.mr_listener = opts.listener;

	/*
	 * This is a reference to a config in the global context object. The
	 * reason we grab this is to stay apprised of updates to the
	 * configuration. The configuration fields that this module uses are:
	 * {
	 *	'record_read_batch_size': number
	 *	'record_read_sort_attr': string
	 *	'record_read_sort_order': string one of ['ASC', 'DESC']
	 *	'record_read_wait_interval': number
	 * }
	 */
	self.mr_cfg = opts.ctx.ctx_moray_cfgs[opts.shard];

	mod_fsm.FSM.call(self, 'running');
};
mod_util.inherits(MorayDeleteRecordReader, mod_fsm.FSM);


MorayDeleteRecordReader.prototype._find_objects = function
_find_objects() {
	var self = this;
	var moray_client = self.mr_ctx.ctx_moray_clients[self.mr_shard];

	/*
	 * This internal method will never be called without a moray client.
	 * This would have been detected in state_running.
	 */
	mod_assertplus.object(moray_client, 'moray_client');

	var find_objects_opts = {
		limit: self.mr_cfg.record_read_batch_size,
		sort: {
			attribute: self.mr_cfg.record_read_sort_attr,
			order: self.mr_cfg.record_read_sort_order
		}
	};

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
	if (!self.mr_ctx.ctx_moray_clients[self.mr_shard]) {
		S.gotoState('waiting');
		return;
	}

	var req = self._find_objects();

	req.on('record', function (record) {
		/*
		 * TODO: Buffering logic?
		 */
		self.mr_listener.emit('record', record);
	});

	req.on('error', function (err) {
		self.mr_log.error('error reading Moray delete records "%s"', err);
		/*
		 * TODO: backoff logic
		 */
		S.gotoState('error');
	});

	req.on('end', function () {
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
	}, self.mr_cfg.record_read_wait_interval);

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

	/*
	 * TODO: backoff logic
	 */
};

module.exports = {
	MorayDeleteRecordReader: MorayDeleteRecordReader
}
