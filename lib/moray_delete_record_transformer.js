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
var mod_path = require('path');
var mod_util = require('util');
var mod_uuidv4 = require('uuid/v4');
var mod_vasync = require('vasync');


function
MorayDeleteRecordTransformer(opts)
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
		component: 'MorayDeleteRecordTransformer'
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

	self.on('record', function (record) {
		mod_vasync.waterfall([
			function transform(next) {
				self._transform(record, next);
			},
			function emit_or_flush(res, next) {
				if (res.zero_byte_obj_key !== null) {
					self.mt_moray_listener.emit('key',
						res.zero_byte_obj_key);
					next();
					return;
				}
				self._flush_mako_instructions(res.storage_ids, next);
			}
		], function (err) {
			if (err) {
				self.mt_log.warn(err, 'error transforming record');
			}
		});
	});
}
mod_util.inherits(MorayDeleteRecordTransformer, mod_events.EventEmitter);


MorayDeleteRecordTransformer.prototype._transform = function
_transform(record, done)
{
	var self = this;
	var mako_cfg = self.mt_ctx.ctx_mako_cfg;

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
	var transformed = [value.creator, value.objectId]

	/*
	 * Zero byte objects, links, and directories don't have
	 * backing files in Mako, so we skip directly to the Moray
	 * cleanup step.
	 */
	if (value.contentLength === 0 || value.type !== 'object') {
		res.zero_byte_obj_key = transfomed.join('/');
		done(null, res);
		return;
	}

	if (value.sharks === undefined || value.sharks.length === 0) {
		done(new VE('unexpected delete record "%s"',
			mod_util.inspect(record)));
		return;
	}

	mod_vasync.forEachParallel({
		inputs: value.sharks,
		func: function cache_instruction(shark, vdone) {
			var storage_id = shark.manta_storage_id;
			var batch_size = mako_cfg.instr_upload_batch_size;

			if (self.mt_cache[storage_id] === undefined) {
				self.mt_cache[storage_id] = [];
			}
			self.mt_cache[storage_id].push(transformed);

			if (self.mt_cache[storage_id].length > batch_size) {
				res.flush.push(storage_id);
			}
			vdone();
		}
	}, function (_) {
		done(null, res);
	});
};


MorayDeleteRecordTransformer.prototype._flush_mako_instructions = function
_flush_mako_instructions(storage_ids, done)
{
	var self = this;

	/*
	 * Emit and flush instructions for the subset of storage ids included
	 * in the storage_ids array.
	 */
	mod_vasync.forEachParallel({
		inputs: storage_ids,
		func: function flush(storage_id) {
			if (self.mt_cache[storage_id] === undefined) {
				self.mt_log.debug('requested flush of empty cache');
				return;
			}
			/*
			 * The actual emission format is an object with a
			 */
			self.mt_mako_listener.emit('instruction', {
				manta_storage_id: storage_id,
				instrs: self.mt_cache[storage_id]
			});
			delete (self.mt_cache[storage_id]);
		}
	}, done);
};


module.exports = {

	MorayDeleteRecordTransformer: MorayDeleteRecordTransformer

};
