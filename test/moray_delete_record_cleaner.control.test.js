/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var mod_assertplus = require('assert-plus');
var mod_events = require('events');
var mod_path = require('path');
var mod_util = require('util');
var mod_uuidv4 = require('uuid/v4');
var mod_vasync = require('vasync');

var lib_testcommon = require('./common');

var MANTA_FASTDELETE_QUEUE = lib_testcommon.MANTA_FASTDELETE_QUEUE;

var TEST_OWNER = mod_uuidv4();

/*
 * This value must be greater than the CLEANER_DELAY below.
 */
var TEST_DELAY = 10000;

var NUM_TEST_RECORDS = 5;

var TEST_RECORDS = (function generate_test_records() {
	var keys = [];

	for (var i = 0; i < NUM_TEST_RECORDS; i++) {
		keys.push([mod_uuidv4(), TEST_OWNER]);
	}

	return (keys);
})();

var TEST_RECORD_KEYS = TEST_RECORDS.map(function (record) {
	return record.join('/');
});


function
do_moray_cleaner_pause_resume_test(test_done)
{
	mod_vasync.waterfall([
		function setup_context(next) {
			lib_testcommon.create_mock_context(function (err, ctx) {
				if (err) {
					console.log('error creating context');
					next(err);
					return;
				}

				var shard = Object.keys(ctx.ctx_moray_clients)[0];
				next(null, ctx, shard);
			});
		},
		function create_and_pause_cleaner(ctx, shard, next) {
			var cleaner = lib_testcommon.create_moray_delete_record_cleaner(ctx,
				shard);
			var received = false;
			var timer = setTimeout(function () {
				if (!received) {
					mod_assertplus.ok(false, 'did not receive ' +
						'paused event');
				}
			}, TEST_DELAY);
			cleaner.once('paused', function () {
				received = true;
				clearTimeout(timer);
				mod_assertplus.ok(cleaner.isInState('paused'),
					'cleaner in unexpected state!');
				next(null, ctx, shard, cleaner);
			});
			cleaner.emit('assertPause');
		},
		function create_delete_records(ctx, shard, cleaner, next) {
			var client = ctx.ctx_moray_clients[shard];
			function create_delete_record(record, cb) {
				ctx.ctx_log.debug('creating test record ' + record);
				lib_testcommon.create_fake_delete_record(ctx,
					client, lib_testcommon.MANTA_FASTDELETE_QUEUE,
					record[1], record[0], [], cb);
			}
			mod_vasync.forEachPipeline({
				inputs: TEST_RECORDS,
				func: create_delete_record
			}, function (err) {
				next(err, ctx, shard, cleaner);
			});
		},
		function clean_delete_records(ctx, shard, cleaner, next) {
			cleaner.emit('cleanup', TEST_RECORD_KEYS);
			setTimeout(function () {
				next(null, ctx, shard, cleaner);
			}, 10000);
		},
		function check_records_still_exist(ctx, shard, cleaner, next) {
			var client = ctx.ctx_moray_clients[shard];
			function get_delete_record(key, cb) {
				lib_testcommon.get_fake_delete_record(
					client, key, cb);
			}
			mod_vasync.forEachPipeline({
				inputs: TEST_RECORD_KEYS,
				func: get_delete_record
			}, function (err) {
				next(err, ctx, shard, cleaner);
			});
		},
		function resume_cleaner(ctx, shard, cleaner, next) {
			var received = false;
			var timer = setTimeout(function () {
				mod_assertplus.ok(false, 'did not ' +
					'receive running event');
			}, TEST_DELAY);
			cleaner.once('running', function () {
				clearTimeout(timer);
				received = true;
				mod_assertplus.ok(cleaner.isInState('running'),
					'cleaner in unexpected state!');
				setTimeout(function () {
					next(null, ctx, shard, cleaner);
				}, TEST_DELAY);
			});
			mod_assertplus.ok(cleaner.isInState('paused'),
				'found cleaner in unexpected state');
			cleaner.emit('assertResume');
		},
		function check_records_dont_exist(ctx, shard, cleaner, next) {
			var client = ctx.ctx_moray_clients[shard];
			function get_delete_record(key, cb) {
				lib_testcommon.get_fake_delete_record(
					client, key, cb);
			}
			mod_vasync.forEachPipeline({
				inputs: TEST_RECORD_KEYS,
				func: get_delete_record
			}, function (_, results) {
				mod_assertplus.ok(results.successes.length === 0,
					'some records still exist');
				next();
			});
		}
	], function (err) {
		if (err) {
			process.exit(1);
		}
		test_done();
	});
}


mod_vasync.pipeline({funcs: [
	function (_, next) {
		do_moray_cleaner_pause_resume_test(next);
	}
]}, function (_) {
	console.log('tests passed');
	process.exit(0);
});
