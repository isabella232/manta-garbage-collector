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
var mod_jsprim = require('jsprim');
var mod_util = require('util');
var mod_vasync = require('vasync');

var mod_actors = require('./actors');

function
GCWorker(opts)
{
	var self = this;

	mod_assertplus.object(opts, 'opts');
	mod_assertplus.string(opts.bucket, 'opts.bucket');
	mod_assertplus.object(opts.ctx, 'opts.ctx');
	mod_assertplus.string(opts.shard, 'opts.shard');
	mod_assertplus.object(opts.log, 'opts.log');

	self.gcw_log = opts.log.child({
		component: 'GCWorker'
	});

	self.gcw_bucket = opts.bucket;

	/*
	 * Overrides for all actors comprising this worker.
	 */
	self.actor_cfg_overrides = {
		log: self.gcw_log
	};

	/*
	 * Options passed to each actor that interfaces with Moray.
	 */
	self.moray_actor_defaults = {
		bucket: self.gcw_bucket
	};

	/*
	 * Options passed to each actor that interfaces with Mako.
	 */
	self.mako_actor_defaults = {};


	/*
	 * The order of the actors included in this pipeline matches the order
	 * in which records are processed.
	 */
	self.gcw_pipeline = [];

	/*
	 * We have a series of interfaces each of which represents a stage in
	 * the GC pipeline.
	 *
	 * MorayDeleteRecordReader - uses the node-moray findobjects API to read
	 * delete records from either the manta_fastdelete_queue or the
	 * manta_delete_log.
	 *
	 * DeleteRecordTransformer - listens for records received by the
	 * MorayDeleteRecordReader and transforms them into instructions
	 * consumable by the MakoInstructionUploader.
	 *
	 * MakoInstructionUploader - listens for instructions transformed by
	 * the MorayDeleteTransformer and uses the node-manta client to to
	 * upload instructions to locations that are well-known by the
	 * corresponding Makos.
	 *
	 * MorayDeleteRecordCleaner - listens for confirmation that delete
	 * instructions have been uploaded to Manta, and then uses a moray
	 * client to delete the corresponding records from the
	 * manta_fastdelete_queue or the manta_delete_log.
	 */
	mod_vasync.waterfall([
		function init_moray_cleaner(next) {
			var moray_cleaner_opts = mod_jsprim.mergeObjects(opts,
				self.actor_cfg_overrides,
				self.moray_actor_defaults);
			var cleaner = new mod_actors.MorayDeleteRecordCleaner(
				moray_cleaner_opts);

			self.gcw_pipeline.unshift(cleaner);
			next(null, cleaner);
		},
		function init_mako_uploader(listener, next) {
			var mako_uploader_opts = mod_jsprim.mergeObjects(opts,
				self.actor_cfg_overrides,
				self.moray_actor_defaults);
			mako_uploader_opts.listener = listener;

			var uploader = new mod_actors.MakoInstructionUploader(
				mako_uploader_opts);

			self.gcw_pipeline.unshift(uploader);

			next(null, listener, uploader);
		},
		function init_record_transformer(moray_listener, mako_listener,
			next) {
			var record_transformer_opts = mod_jsprim.mergeObjects(
				opts, self.actor_cfg_overrides,
				self.moray_actor_defaults);

			/*
			 * The transformer is where we split off zero and
			 * non-zero byte objects. The former don't need to have
			 * Mako instructions uploaded.
			 */
			record_transformer_opts.moray_listener = moray_listener;
			record_transformer_opts.mako_listener = mako_listener;

			var transformer =
				new mod_actors.DeleteRecordTransformer(
				record_transformer_opts);

			self.gcw_pipeline.unshift(transformer);

			next(null, transformer);
		},
		function init_moray_delete_record_reader(listener, next) {
			var record_reader_opts = mod_jsprim.mergeObjects(opts,
				self.actor_cfg_overrides,
				self.moray_actor_defaults);

			record_reader_opts.listener = listener;

			var reader = new mod_actors.MorayDeleteRecordReader(
				record_reader_opts);

			self.gcw_pipeline.unshift(reader);

			next();
		}
	], function () {
		mod_fsm.FSM.call(self, 'running');
	});
}
mod_util.inherits(GCWorker, mod_fsm.FSM);


GCWorker.prototype.state_running = function
state_running(S)
{
	var self = this;

	S.on(self, 'pause', function () {
		self.gcw_log.debug('pausing worker');
		for (var i = 0; i < self.gcw_pipeline.length; i++) {
			self.gcw_pipeline[i].emit('pause');
		}
		S.gotoState('paused');
	});

	S.on(self, 'shutdown', function () {
		S.gotoState('shutdown');
	});

	self.emit('running');
};


GCWorker.prototype.state_paused = function
state_paused(S)
{
	var self = this;

	S.on(self, 'resume', function () {
		for (var i = 0; i < self.gcw_pipeline.length; i++) {
			self.gcw_pipeline[i].emit('resume');
		}
		S.gotoState('running');
	});

	S.on(self, 'shutdown', function () {
		S.gotoState('shutdown');
	});

	self.emit('paused');
};


GCWorker.prototype.state_shutdown = function
state_shutdown(S)
{
	var self = this;

	var actors = self.gcw_pipeline;

	for (var i = 0; i < actors.length; i++) {
		var actor = actors[i];
		actor.emit('shutdown');
	}
};


GCWorker.prototype.shutdown = function
shutdown()
{
	var self = this;

	self.emit('shutdown');
};


GCWorker.prototype.describe = function
describe()
{
	var self = this;

	var descr = {
		component: 'worker',
		state: self.getState(),
		actors: self.gcw_pipeline.map(function (actor) {
			return (actor.describe());
		})
	};

	return (descr);
};


function create_gc_worker(opts) {
	return (new GCWorker(opts));
}


module.exports = {

	create_gc_worker: create_gc_worker,

	GCWorker: GCWorker

};
