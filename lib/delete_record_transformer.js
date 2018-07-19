/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

/*
 * Translates and batches delete records intro instructions that can be uploaded
 * to Manta and interpreted by the ordinary Mako gc pipeline.
 */

var mod_assertplus = require('assert-plus');
var mod_events = require('events');
var mod_fsm = require('mooremachine');
var mod_path = require('path');
var mod_util = require('util');
var mod_uuidv4 = require('uuid/v4');
var mod_vasync = require('vasync');
var mod_verror = require('verror');

var VE = mod_verror.VError;


function
DeleteRecordTransformer(opts)
{
	var self = this;

	mod_assertplus.object(opts, 'opts');
	mod_assertplus.object(opts.ctx, 'opts.ctx');
	mod_assertplus.object(opts.ctx.ctx_mako_cfg, 'opts.ctx_mako_cfg');
	mod_assertplus.object(opts.log, 'opts.log');
	mod_assertplus.object(opts.moray_listener, 'opts.moray_listener');
	mod_assertplus.object(opts.mako_listener, 'opts.mako_listener');

	self.mt_ctx = opts.ctx;
	self.mt_log = opts.log.child({
		component: 'DeleteRecordTransformer'
	});
	self.mt_mako_listener = opts.mako_listener;

	/*
	 * Zero byte objects and directories do not need to be funneled through
	 * the Mako uploader process, so we can pass them off directly to the
	 * MorayDeleteRecordCleaner.
	 */
	self.mt_moray_listener = opts.moray_listener;

	/*
	 * A cache of objects to be written out. This is a map from storage_id
	 * to list of identifiers representing the objects to be deleted from
	 * that storage node.
	 */
	self.mt_cache = {};

	mod_fsm.FSM.call(self, 'running');
}
mod_util.inherits(DeleteRecordTransformer, mod_fsm.FSM);


DeleteRecordTransformer.prototype._listen_for_records = function
_listen_for_records()
{
	var self = this;

	self.on('record', function (record) {
		mod_vasync.waterfall([
			function transform(next) {
				self._transform(record, next);
			},
			function emit(res, next) {
				if (res.zero_byte_obj_key !== null) {
					self.mt_log.debug({
						key: res.zero_byte_obj_key
					}, 'emitting zero-byte object cleanup' +
					'request');
					self.mt_moray_listener.emit('cleanup',
						res.zero_byte_obj_key);
					next();
					return;
				}
				if (res.storage_ids.length > 0) {
					self.mt_log.debug({
						sharks: res.storage_ids
					}, 'flushing instructions for ' +
					'storage nodes');
					self._flush_mako_instructions(
						res.storage_ids,
						next);
				}
			}
		], function (err) {
			if (err) {
				self.mt_log.warn(err, 'error transforming ' +
					'record');
			}
		});
	});
};


DeleteRecordTransformer.prototype._stop_listening_for_records = function
_stop_listening_for_records()
{
	var self = this;

	self.removeAllListeners('record');
};


DeleteRecordTransformer.prototype.state_running = function
state_running(S)
{
	var self = this;

	self._start_periodic_instr_flush();
	self._listen_for_records();

	S.on(self, 'assertPause', function () {
		self.mt_log.debug('pausing delete record transformer');
		S.gotoState('paused');
	});

	S.on(self, 'assertShutdown', function () {
		self._stop_periodic_instr_flush();
		self._stop_listening_for_records();
		S.gotoState('shutdown');
	});

	self.emit('running');
};


DeleteRecordTransformer.prototype.state_paused = function
state_paused(S)
{
	var self = this;

	self._stop_periodic_instr_flush();
	self._stop_listening_for_records();

	S.on(self, 'assertResume', function () {
		S.gotoState('running');
	});

	S.on(self, 'assertShutdown', function () {
		S.gotoState('shutdown');
	});

	self.emit('paused');
};


DeleteRecordTransformer.prototype.state_shutdown = function
state_shutdown(S)
{
	var self = this;
	self.emit('shutdown');
};


DeleteRecordTransformer.prototype._get_mako_cfg = function
_get_mako_cfg()
{
	var self = this;

	return (self.mt_ctx.ctx_mako_cfg);
};


DeleteRecordTransformer.prototype._get_mako_instr_upload_batch_size = function
_get_mako_instruction_upload_batch_size()
{
	var self = this;

	return (self._get_mako_cfg().instr_upload_batch_size);
};


DeleteRecordTransformer.prototype._get_mako_instr_upload_flush_delay = function
_get_mako_instr_upload_flush_delay()
{
	var self = this;

	return (self._get_mako_cfg().intsr_upload_flush_delay);
};


DeleteRecordTransformer.prototype._transform = function
_transform(record, done)
{
	var self = this;

	self.mt_log.debug({
		record: mod_util.inspect(record)
	}, 'received record');

	var value = record.value;
	var res = {
		storage_ids: [],
		zero_byte_obj_key: null
	};

	/*
	 * We turn each record into a string that consists of
	 * its creator and object IDs. These are the two pieces
	 * of information needed to identify the corresponding
	 * backing file on a Mako. The instructions are
	 * aggregated into a list and flushed to the listener in
	 * batches.
	 */
	var transformed = [value.creator, value.objectId];

	/*
	 * Zero byte objects, links, and directories don't have
	 * backing files in Mako, so we skip directly to the Moray
	 * cleanup step.
	 */
	if (value.contentLength === 0 || value.type !== 'object') {
		res.zero_byte_obj_key = record.key;
		done(null, res);
		return;
	}

	if (value.sharks === undefined || value.sharks.length === 0) {
		done(new VE('unexpected delete record "%s"',
			mod_util.inspect(record)));
		return;
	}

	self.mt_log.debug({
		sharks: value.sharks
	}, 'caching instructions for sharks');

	mod_vasync.forEachParallel({
		inputs: value.sharks,
		func: function cache_instruction(shark, vdone) {
			var storage_id = shark.manta_storage_id;

			if (self.mt_cache[storage_id] === undefined) {
				self.mt_cache[storage_id] = [];
			}
			self.mt_cache[storage_id].push({
				key: record.key,
				transformed: transformed
			});

			/*
			 * Collect storage ids that have reached the treshold
			 * batch size.
			 */
			if (self.mt_cache[storage_id].length >=
				self._get_mako_instr_upload_batch_size()) {
				self.mt_log.debug({
					shark: storage_id
				}, 'requesting flush');
				res.storage_ids.push(storage_id);
			}
			vdone();
		}
	}, function (_) {
		done(null, res);
	});
};


DeleteRecordTransformer.prototype._start_periodic_instr_flush = function
_start_periodic_instr_flush()
{
	var self = this;

	if (self.mt_periodic_flush_enabled) {
		return;
	}
	self.mt_periodic_flush_enabled = true;

	function flush() {
		if (!self.mt_periodic_flush_enabled) {
			return;
		}
		self._flush_mako_instructions(Object.keys(self.mt_cache),
				function (err) {
			if (err) {
				self.mt_log.error({
					err: err
				}, 'failed periodic instruction flush');
			}
			setTimeout(flush,
				self._get_mako_instr_upload_flush_delay());
		});
	}

	setTimeout(flush, self._get_mako_instr_upload_flush_delay());
};


DeleteRecordTransformer.prototype._stop_periodic_instr_flush = function
_stop_periodic_instr_flush()
{
	var self = this;

	if (!self.mt_periodic_flush_enabled) {
		return;
	}

	self.mt_periodic_flush_enabled = false;
};


DeleteRecordTransformer.prototype._flush_mako_instructions = function
_flush_mako_instructions(storage_ids, done)
{
	var self = this;

	if (self.mt_mako_flush_in_progress) {
		done();
		return;
	}

	self.mt_mako_flush_in_progress = true;

	var flush_done = function (err) {
		self.mt_mako_flush_in_progress = false;
		done(err);
	};

	/*
	 * Emit and flush instructions for the subset of storage ids included
	 * in the storage_ids array.
	 */
	mod_vasync.forEachParallel({
		inputs: storage_ids,
		func: function flush(storage_id, finished) {
			if ((self.mt_cache[storage_id] || []).length === 0) {
				self.mt_log.debug('requested flush ' +
					'of empty cache');
				return;
			}

			self.mt_log.debug({
				manta_storage_id: storage_id,
				count: self.mt_cache[storage_id].length
			}, 'emitting mako instructions');

			self.mt_mako_listener.emit('instruction', {
				manta_storage_id: storage_id,
				instrs: self.mt_cache[storage_id]
			});

			delete (self.mt_cache[storage_id]);
			finished();
		}
	}, flush_done);
};


DeleteRecordTransformer.prototype.describe = function
describe()
{
	var self = this;

	var descr = {
		component: 'transformer',
		state: self.getState()
	};

	return (descr);
};


module.exports = {

	DeleteRecordTransformer: DeleteRecordTransformer

};
