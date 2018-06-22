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
var mod_util = require('util');
var mod_uuidv4 = require('uuid/v4');
var mod_vasync = require('vasync');

var lib_testcommon = require('./common');


var TEST_OWNER = mod_uuidv4();
var TEST_OBJECTID = mod_uuidv4();
var DELAY = 3000;

function
create_fake_delete_record(ctx, client, objectId, done)
{
	var value = {
		dirname: 'manta_gc_test',
		key: TEST_OWNER + '/' + objectId,
		headers: {},
		mtime: Date.now(),
		name: 'manta_gc_test_obj',
		creator: TEST_OWNER,
		owner: TEST_OWNER,
		objectId: objectId,
		roles: [],
		type: 'object',
		vnode: 1234
	};
	client.putObject(lib_testcommon.MANTA_FASTDELETE_QUEUE, value.key,
		value, {}, function (err) {
		if (err) {
			ctx.ctx_log.error(err, 'unable to create test object');
			process.exit(1);
		}
		done();
	});
}

(function
main()
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

				ctx.ctx_moray_cfgs[shard] = {
					record_read_batch_size: 1,
					record_read_sort_attr: '_mtime',
					record_read_sort_order: 'DESC',
					record_read_wait_interval: 1000
				};

				next(null, ctx, shard);
			});
		},
		function create_delete_record(ctx, shard, next) {
			create_fake_delete_record(ctx, ctx.ctx_moray_clients[shard],
				TEST_OBJECTID, function (err) {
				if (err) {
					ctx.ctx_log.error(err, 'unabled to create delete record');
					next(err);
					return;
				}
				next(null, ctx, shard);
			});
		},
		function read_delete_record(ctx, shard, next) {
			var key = TEST_OWNER + '/' + TEST_OBJECTID;
			var listener = new mod_events.EventEmitter();
			var reader = lib_testcommon.create_moray_delete_record_reader(ctx,
				shard, listener);

			var timer = setTimeout(function () {
				listener.removeAllListeners('record');
				mod_assertplus.ok(false, 'did not receive record event');
				next();
			}, DELAY);

			listener.once('record', function (record) {
				clearTimeout(timer);
				ctx.ctx_log.debug({
					record: mod_util.inspect(record)
				}, 'received record');
				mod_assertplus.equal(record.key, key, 'unexpected key ' +
					'from record sent to listener');
				ctx.ctx_log.info('test passed');
				next();
			});
		}
	], function (err) {
		if (err) {
			process.exit(1);
		}
		process.exit(0);
	});
})();
