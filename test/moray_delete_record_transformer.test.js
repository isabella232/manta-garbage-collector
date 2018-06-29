/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var mod_vasync = require('vasync');
var mod_uuidv4 = require('uuid/v4');


var TEST_OWNER = mod_uuidv4();
var TEST_OBJECTID = mod_uuidv4();
var TEST_ZERO_BYTE_OBJECTID = mod_uuidv4();
var DELAY = 1000;


var TEST_OBJECT_RECORD = {
	dirname: 'manta_gc_test',
	key: TEST_OWNER + '/' + TEST_OBJECTID,
	headers: {},
	mtime: Date.now(),
	name: 'manta_gc_test_obj',
	creator: TEST_OWNER,
	owner: TEST_OWNER,
	objectId: TEST_OBJECTID,
	roles: [],
	type: 'object',
	vnode: 1234,
	contentLength: 512000,
	sharks: [
		{
			datacenter: 'dc',
			manta_storage_id: '1.stor.orbit.example.com'
		},
		{
			datacenter: 'dc',
			manta_storage_id: '2.stor.orbit.example.com'
		}
	]
};

var TEST_OBJECT_SHARKS = TEST_OBJECT_RECORD.sharks.map(function (shark) {
	return (shark.manta_storage_id);
});

var TEST_ZERO_BYTE_OBJECT_RECORD = {
	dirname: 'manta_gc_test',
	key: TEST_OWNER + '/' + TEST_ZERO_BYTE_OBJECTID,
	headers: {},
	mtime: Date.now(),
	name: 'manta_gc_test_zero_byte_obj',
	creator: TEST_OWNER,
	owner: TEST_OWNER,
	objectId: TEST_ZERO_BYTE_OBJECTID,
	roles: [],
	type: 'object',
	vnode: 1234,
	contentLength: 0,
	sharks: []
};


(function
 main()
 {
	 mod_vasync.waterfall([
		function setup_context(next) {
			lib_testcommon.ceate_mock_context(function (err, ctx) {
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

				ctx.ctx_mako_cfg = {
					instr_upload_batch_size: 1
				}

				next(null, ctx, shard);
			});
		},
		function create_transformer(ctx, shard, next) {
			var listeners = {
				moray_listener: new mod_events.EventEmitter(),
				mako_listener: new mod_events.EventEmitter()
			};
			var transfomer = lib_testcommon.create_moray_delete_record_transformer(
					ctx, shard, listeners);
			next(transformer, listeners, ctx, shard);
		},
		function check_transformed_records(transfomer, listeners, ctx, shard, next) {
			var received_zero_byte_obj = false;
			var received_mako_backed_obj = false;

			var storage_ids_received = [];
			var instrs_received = {};

			listeners.moray_listener.on('key', function (key) {
				mod_assertplus.ok(false, 'received moray cleaner event' +
					'for mako-backed object');
			});

			listeners.mako_listener.on('instruction', function (instr) {
				mod_assertplus.ok(!received_zero_byte_obj, 'received mako-backed' +
					'object instructions after zero byte object key');

				mod_assertplus.object(instr, 'instr');
				mod_assertplus.string(instr.manta_storage_id,
					'instr.manta_storage_id');
				mod_assertplus.arrayOfString(instr.instrs,
					'instr.insts');

				var storage_id = instr.manta_storage_id;
				var instrs = instr.instrs;
				var batch_size = ctx.ctx_mako_cfg.instr_upload_batch_size

				mod_assertplus.ok(instrs.length === batch_size, 'unexpected ' +
					'mako instruction count');

				storage_ids_received.push(storage_id);
				instrs_received[storage_id] = instrs;
			});
			/*
			 * Emulate the results of the MorayDeleteRecordReader.
			 */
			transfomer.emit('record', TEST_OBJECT_RECORD);

			/*
			 * Check that for each shark on which the record is
			 * stored, an instruction to delete the corresponding
			 * object from that shark was received.
			 */
			setTimeout(function () {
				mod_assertplus.equal(TEST_OBJECT_SHARKS.length,
					storage_nodes_received.length, 'shark number ' +
					'mismatch');
				TEST_OBJECT_SHARKS.forEach(function (storage_id) {
					mod_assertplus.ok(storage_ids_received.indexOf(
						storage_id) !== -1, 'missing expected mako ' +
						'instructions');

					function check_line_fmt(line) {
						mod_assertplus.equal(lines[0],
							TEST_OWNER, 'instr owner mismatch');
						mod_assertplus.equal(lines[1],
							TEST_OBJECTID, 'instr object id mismatch');
					}

					function check_instrs_fmt(lines) {
						lines.forEach(check_line_fmt);
					}

					for (var i = 0; i < instrs_received[storage_id].length;
						i++) {
						check_instrs_fmt(instrs_received[storage_id][i]);
					}
					listeners.mako_listener.removeAllListeners(
						'instruction');
					listeners.moray_listener.removeAllListeners(
						'key');
				});
			}, DELAY);

		}, function (transformer, listeners, ctx, shard, next) {
			var received_key = false;
			liseteners.mako_listener.on('instruction', function (instr) {
				mod_assetplus.ok(false, 'received mako instruction for zero ' +
					'byte object');
			});

			listeners.moray_listener.on('key', function (key) {
				received_key = true;
				mod_assetplus.equals(key, mod_path.join(TEST_OWNER,
					TEST_ZERO_BYTE_OBJECTID), 'zero byte object key ' +
					'mismatch');
				next();
			});

			setTimeout(function () {
				mod_assertplus.ok(received_key, 'did not receive zero byte ' +
					'object key');
				listeners.mako_listener.removeAllListeners('instruction');
				listeners.moray_listener.removeAllListeners('key');
				next();
			}, DELAY);
		}
	], function (err) {
		if (err) {
			process.exit(1);
		}
		console.log('tests passed');
		process.exit(0);
	});
 });
