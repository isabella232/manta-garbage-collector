/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2019, Joyent, Inc.
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

	/*
	 * A count of the total number of entries resident in `mt_cache`. We
	 * compare this against the capacity tunable.
	 */
	self.mt_cache_count = 0;

	self.mt_last_flush = Date.now();

	mod_fsm.FSM.call(self, 'running');
}
mod_util.inherits(DeleteRecordTransformer, mod_fsm.FSM);


DeleteRecordTransformer.prototype._get_tunables_ref = function
_get_tunables_ref()
{
	return (this.mt_ctx.ctx_cfg.tunables);
};


DeleteRecordTransformer.prototype._get_collector = function
_get_collector()
{
	return (this.mt_ctx.ctx_metrics_manager.collector);
};


DeleteRecordTransformer.prototype._get_batch_size = function
_get_batch_size()
{
	return (this._get_tunables_ref().instr_upload_batch_size);
};


DeleteRecordTransformer.prototype._get_delay = function
_get_delay()
{
	return (this._get_tunables_ref().instr_upload_flush_delay);
};


DeleteRecordTransformer.prototype._get_cache_capacity = function
_get_cache_capacity()
{
	return (this._get_tunables_ref().capacity);
};


DeleteRecordTransformer.prototype._incr_cache_counts = function
_incr_cache_counts()
{
	var self = this;

	self.mt_cache_count++;
	self.mt_ctx.ctx_total_cache_entries++;

	if (self.mt_ctx.ctx_metrics_manager) {
		self._get_collector().getCollector('gc_cache_entries').set(
			self.mt_ctx.ctx_total_cache_entries);
	}
};


DeleteRecordTransformer.prototype._decr_cache_counts = function
_decr_cache_counts(delta)
{
	var self = this;

	self.mt_cache_count -= delta;
	self.mt_ctx.ctx_total_cache_entries -= delta;

	if (self.mt_ctx.ctx_metrics_manager) {
		self._get_collector().getCollector('gc_cache_entries').set(
			self.mt_ctx.ctx_total_cache_entries);
	}
};


DeleteRecordTransformer.prototype._get_total_cache_entries = function
_get_total_cache_entries()
{
	var self = this;
	return (self.mt_ctx.ctx_total_cache_entries);
};


/*
 * Accepts a delete record and invokes the callback with either a key, in the
 * case of a zero byte object, or a list of sharks for which we have reached
 * that cache threshold.
 */
DeleteRecordTransformer.prototype._process_record = function
_process_record(record, done)
{
	var self = this;

	var value = record.value;
	var key = record.key;
	var sharks = [];

	/*
	 * Reminder: As discussed earlier, object metadata for objects created
	 * with the Manta multipart upload API do not have creator uuids.  In
	 * such a case, we can obtain the uuid from the `owner' field instead.
	 * Failure to do so will prevent us from deleting the object later on
	 * down the road because we will not have a means of associating the
	 * object with an account (a necessary requirement for object deletion
	 * on a mako).  On a more positive note, this is the last stop in the
	 * chain where it is necessary to dig through a record for this kind of
	 * information.  From here on out, account information will be passed
	 * forward via `line_data'.
	 */
	var creator = value.creator || value.owner;
	var line_data = [creator, value.objectId];

	if (value.type !== 'object' || value.contentLength === 0) {
		done(null, null, key);
		return;
	}

	if (value.sharks === undefined || value.sharks.length === 0) {
		done(new VE('Unexpected delete record for key "%s" is a ' +
			'non-zero-byte object with no "sharks".', record.key));
		return;
	}

	/*
	 * The DeleteRecordTransformer emits a separate instruction for each
	 * storage node the object referred to by the record's key is stored on.
	 * Since the metadata record occupies a single row in the metadata tier,
	 * we use this bit of state to ensure that the key is included in only
	 * one batch delete.
	 *
	 * A Moray batch delete could fail a potentially large number of
	 * requests if any given delete transaction fails, so not doing this
	 * opens the collector up to potentially wasting a lot of work.
	 */
	var cleaned_state = {
		cleaned: false
	};

	mod_vasync.forEachParallel({
		inputs: value.sharks.map(function (obj) {
			return (obj.manta_storage_id);
		}),
		func: function cache_instruction(storage_id, finished) {
			var cache = self.mt_cache;

			if (self._get_total_cache_entries() >
			    self._get_cache_capacity()) {

				self.mt_log.debug({
					record: mod_util.inspect(record),
					total_cache_entries:
						self._get_total_cache_entries(),
					cache_capacity:
						self._get_cache_capacity()
				}, 'Skipping record due to cache bloat.');
				finished();
				return;
			}

			if (!cache.hasOwnProperty(storage_id)) {
				cache[storage_id] = {};
			}

			if (!cache[storage_id].hasOwnProperty(record.key)) {
				cache[storage_id][record.key] = {
					key: record.key,
					line: line_data,
					size: value.contentLength,
					sharks: value.sharks,
					cleaned_state: cleaned_state
				};
				self._incr_cache_counts();
			}


			if (Object.keys(cache[storage_id]).length >=
			    self._get_batch_size()) {
				sharks.push(storage_id);
			}
			finished();
		}
	}, function (_) {
		done(null, sharks);
	});
};


DeleteRecordTransformer.prototype._flush = function
_flush(storage_ids, done)
{
	var self = this;

	if (self.mt_mako_flush_in_progress) {
		done();
		return;
	}

	self.mt_mako_flush_in_progress = true;


	var flush_done = function (err) {
		if (!err) {
			self.mt_last_flush = Date.now();
		}
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
			var lines = Object.keys(self.mt_cache[storage_id]).map(
				function (key) {
				return (self.mt_cache[storage_id][key]);
			});

			if ((lines || []).length === 0) {
				finished();
				return;
			}

			self.mt_log.info({
				storage_id: storage_id,
				lines: lines.length
			}, 'Begin flushing records for mako consumption.');

			/*
			 * It is unfortunate and inefficient that we have to
			 * micromanage GC like this, but it has been observed
			 * that account information is occasionally omitted
			 * from instructions.  If we see that either the
			 * account (elem.line[0]) or the object (elem.line[1])
			 * is not present, log the occurance of this atrocity
			 * for future analysis.
			 */
			lines.forEach(function (elem) {
				if (!elem.line[0] || !elem.line[1]) {
					self.mt_log.error({elem: elem},
					    'DeleteRecordTransformer: '+
					    'missing information.');
				}
			});

			self.mt_mako_listener.emit('instruction', {
				storage_id: storage_id,
				lines: lines
			});

			var delta = Object.keys(self.mt_cache[
				storage_id]).length;
			self._decr_cache_counts(delta);
			delete (self.mt_cache[storage_id]);

			finished();
		}
	}, flush_done);
};


DeleteRecordTransformer.prototype._start_periodic_instr_flush = function
_start_periodic_instr_flush()
{
	var self = this;

	if (self.mt_periodic_flush_enabled) {
		return;
	}
	self.mt_periodic_flush_enabled = true;

	function tick() {
		if (!self.mt_periodic_flush_enabled) {
			return;
		}

		if ((Date.now() - self.mt_last_flush) < self._get_delay()) {
			self.mt_log.debug({
				last_flush: self.mt_last_flush,
				delay: self._get_delay()
			}, 'Skipping periodic record flush.');
			setTimeout(tick, self._get_delay());
			return;
		}

		self._flush(Object.keys(self.mt_cache), function (err) {
			if (err) {
				self.mt_log.error({
					err: err.message
				}, 'Error encountered while periodically ' +
				'flushing instructions.');
			}
			setTimeout(tick, self._get_delay());
		});
	}

	setTimeout(tick, self._get_delay());
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


DeleteRecordTransformer.prototype._listen_for_records = function
_listen_for_records()
{
	var self = this;

	self.on('record', function (record) {
		self._process_record(record, function (err, sharks, key) {
			if (err) {
				self.mt_log.warn({
					err: err.message
				}, 'Error while processing record.');
				return;
			}
			if (key) {
				/*
				 * We do not expect to read zero byte objects
				 * from the manta_fastdelete_queue because the
				 * Moray delete trigger doesn't insert such
				 * records in the queue. If for some reason we
				 * do find such records in the queue, they are
				 * removed.
				 */
				self.mt_moray_listener.emit('cleanup', {
					key: key,
					size: 0,
					sharks: []
				});
				return;
			}
			if (sharks.length === 0) {
				self.mt_log.info('Skipping no-op flush.');
				return;
			}
			self._flush(sharks, function (ferr) {
				if (ferr) {
					self.mt_log.warn({
						sharks: sharks,
						err: ferr.message
					}, 'Error flushing records.');
				}
			});
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
		self.mt_log.debug('Pausing delete record transformer.');
		S.gotoState('paused');
	});

	S.on(self, 'assertResume', function () {
		self.emit('running');
	});

	S.on(self, 'assertShutdown', function () {
		self._stop_periodic_instr_flush();
		self._stop_listening_for_records();
		S.gotoState('shutdown');
	});

	self.emit('running');
};


DeleteRecordTransformer.prototype.state_paused = function
state_paused(S) {
	var self = this;

	self._stop_periodic_instr_flush();
	self._stop_listening_for_records();

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


DeleteRecordTransformer.prototype.state_shutdown = function
state_shutdown(S)
{
	var self = this;
	self.emit('shutdown');

	S.on(self, 'assertShutdown', function () {
		self.mt_log.debug('Received shutdown event twice!');
	});
};


DeleteRecordTransformer.prototype.describe = function
describe()
{
	var self = this;

	var descr = {
		component: 'transformer',
		state: self.getState(),
		cached: self.mt_cache_count
	};

	return (descr);
};


module.exports = {

	DeleteRecordTransformer: DeleteRecordTransformer

};
