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
		keys.push([TEST_OWNER, mod_uuidv4()]);
	}

	return (keys);
})();

var TEST_RECORD_KEYS = TEST_RECORDS.map(function (record) {
	return record.join('/');
});

/*
 * num_records is the number of records we will request that the cleaner get rid
 * of. Passing this as an argument allows us to easily test that the cleaner is
 * still deleting records when the cache size stays below the batch size for a
 * configurable delay (see `record_delete_delay`).
 */
function
do_moray_cleaner_test(num_records, test_done)
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
		function create_delete_records(ctx, shard, next) {
			var client = ctx.ctx_moray_clients[shard];
			function create_delete_record(record, cb) {
				ctx.ctx_log.debug('creating test record ' + record);
				lib_testcommon.create_fake_delete_record(ctx,
					client, lib_testcommon.MANTA_FASTDELETE_QUEUE,
					record[0], record[1], [], cb);
			}
			mod_vasync.forEachPipeline({
				inputs: TEST_RECORDS.slice(num_records),
				func: create_delete_record
			}, function (err) {
				next(err, ctx, shard);
			});
		},
		function clean_delete_records(ctx, shard, next) {
			var cleaner = lib_testcommon.create_moray_delete_record_cleaner(ctx,
				shard);
			cleaner.emit('cleanup', TEST_RECORD_KEYS.slice(num_records));
			setTimeout(function () {
				next(null, ctx, shard);
			}, TEST_DELAY);
		},
		function check_delete_records_cleaned(ctx, shard, next) {
			var client = ctx.ctx_moray_clients[shard];
			function check_for_delete_record(key, cb) {
				client.getObject(MANTA_FASTDELETE_QUEUE, key, function (err, obj) {
					console.log(mod_util.inspect(obj));
					mod_assertplus.ok(err, 'moray cleaner did not remove ' +
						'object');
					cb();
				});
			}
			mod_vasync.forEachParallel({
				inputs: TEST_RECORD_KEYS.slice(num_records),
				func: check_for_delete_record
			}, function (err) {
				next(err);
			});
		}
	], function (err) {
		if (err) {
			process.exit(0);
		}
		test_done();
	});
}

mod_vasync.pipeline({funcs: [
	function (_, next) {
		do_moray_cleaner_test(TEST_RECORDS.length, next);
	},
	function (_, next) {
		do_moray_cleaner_test(TEST_RECORDS.length - 1, next);
	}
]}, function (_) {
	console.log('tests passed');
	process.exit(0);
});
