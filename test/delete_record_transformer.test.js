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
var mod_vasync = require('vasync');
var mod_util = require('util');
var mod_uuidv4 = require('uuid/v4');

var lib_testcommon = require('./common');


var TEST_OWNER = mod_uuidv4();
var TEST_OBJECTID = mod_uuidv4();
var TEST_ZERO_BYTE_OBJECTID = mod_uuidv4();
var DELAY = 5000;

var NUM_TEST_RECORDS = 5;


var TEST_OBJECT_SHARKS = [
	'1.stor.orbit.example.com',
	'2.stor.orbit.example.com'
];

function construct_record(objectid) {
	return {
		key: TEST_OWNER + '/' + objectid,
		value: {
			dirname: 'manta_gc_test',
			key: TEST_OWNER + '/' + objectid,
			headers: {},
			mtime: Date.now(),
			name: 'manta_gc_test_obj',
			creator: TEST_OWNER,
			owner: TEST_OWNER,
			objectId: objectid,
			roles: [],
			type: 'object',
			vnode: 1234,
			contentLength: 512000,
			sharks: TEST_OBJECT_SHARKS.map(function (storage_id) {
				return {
					datacenter: 'dc',
					manta_storage_id: storage_id
				};
			})
		},
		_id: 21,
		_etag: null,
		_mtime: Date.now(),
		_txn_snap: null,
		_count: 1
	};
};

var TEST_OBJECT_RECORDS = (function generate_records () {
	var records = [];
	for (var i = 0; i < NUM_TEST_RECORDS; i++) {
		records.push(construct_record(mod_uuidv4()));
	}
	return (records);
})();

var TEST_OBJECTIDS = TEST_OBJECT_RECORDS.map(function (record) {
	return (record.value.objectId);
});

function construct_zero_byte_object_record(objectid) {
	return {
		key: TEST_OWNER + '/' + objectid,
		value: {
			dirname: 'manta_gc_test',
			headers: {},
			mtime: Date.now(),
			name: 'manta_gc_test_zero_byte_obj',
			creator: TEST_OWNER,
			owner: TEST_OWNER,
			objectId: objectid,
			roles: [],
			type: 'object',
			vnode: 1234,
			contentLength: 0,
			sharks: []
		},
		_id: 22,
		_etag: null,
		_mtime: Date.now(),
		_txn_snap: null,
		_count: 1
	};
}

var TEST_ZERO_BYTE_OBJECT_RECORDS = (function generate_records() {
	var records = [];
	for (var i = 0; i < NUM_TEST_RECORDS; i++) {
		records.push(construct_zero_byte_object_record(mod_uuidv4()));
	}
	return (records);
})();

var TEST_ZERO_BYTE_OBJECTIDS = TEST_ZERO_BYTE_OBJECT_RECORDS.map(function (record) {
	return (record.value.objectId);
});

var TEST_ZERO_BYTE_OBJECT_KEYS = TEST_ZERO_BYTE_OBJECT_RECORDS.map(function (record) {
	return (record.key);
});


function
run_delete_record_transformer_test(num_records, test_done)
{
	mod_assertplus.ok(num_records <= NUM_TEST_RECORDS, 'must test emission of at ' +
		'most NUM_TEST_RECORDS');
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
		function create_transformer(ctx, shard, next) {
			var listeners = {
				moray_listener: new mod_events.EventEmitter(),
				mako_listener: new mod_events.EventEmitter()
			};
			var transformer = lib_testcommon.create_delete_record_transformer(
					ctx, shard, listeners);
			next(null, transformer, listeners, ctx, shard);
		},
		function check_transformed_records(transformer, listeners, ctx, shard, next) {
			var received_zero_byte_obj = false;
			var received_mako_backed_obj = false;

			var storage_ids_received = [];
			var instrs_received = {};

			listeners.moray_listener.on('cleanup', function (key) {
				mod_assertplus.ok(false, 'received moray cleaner event' +
					'for mako-backed object');
			});

			listeners.mako_listener.on('instruction', function (instr) {
				mod_assertplus.ok(!received_zero_byte_obj, 'received mako-backed' +
					'object instructions after zero byte object key');
				mod_assertplus.object(instr, 'instr');
				mod_assertplus.string(instr.storage_id,
					'instr.storage_id');
				mod_assertplus.array(instr.lines, 'instr.lines');

				var storage_id = instr.storage_id;
				var lines = instr.lines;
				var batch_size = ctx.ctx_mako_cfg.instr_upload_batch_size

				mod_assertplus.equal(lines.length, num_records, 'unexpected ' +
					'mako instruction count');

				storage_ids_received.push(storage_id);
				if (instrs_received[storage_id] === undefined) {
					instrs_received[storage_id] = [];
				}
				instrs_received[storage_id] = instrs_received[storage_id].concat(
					lines);
			});

			/*
			 * Emulate the results of the MorayDeleteRecordReader.
			 */
			TEST_OBJECT_RECORDS.slice(0, num_records).forEach(function (record) {
				transformer.emit('record', record);
			});

			/*
			 * Check that for each shark on which the record is
			 * stored, an instruction to delete the corresponding
			 * object from that shark was received.
			 */
			setTimeout(function () {
				listeners.mako_listener.removeAllListeners(
					'instruction');
				listeners.moray_listener.removeAllListeners(
					'cleanup');
				mod_assertplus.equal(TEST_OBJECT_SHARKS.length,
					storage_ids_received.length, 'shark number ' +
					'mismatch');

				for (var i = 0; i < TEST_OBJECT_SHARKS.length; i++) {
					var object_ids_received = {};
					var storage_id = TEST_OBJECT_SHARKS[i];
					mod_assertplus.ok(storage_ids_received.indexOf(
						storage_id) !== -1, 'missing expected mako ' +
						'instructions');

					function check_instr_fmt(instr) {
						mod_assertplus.equal(instr[0],
							TEST_OWNER, 'instr creator mismatch');
						mod_assertplus.uuid(instr[1], 'expected object' +
							'uuid');
						mod_assertplus.ok(object_ids_received[instr[1]]
							=== undefined, 'received instruction ' +
							'for the same objectid twice');
						mod_assertplus.ok(TEST_OBJECTIDS.slice(0,
							num_records).indexOf(instr[1])
							!== -1, 'unexpected object id');
						object_ids_received[instr[1]] = true;
					}

					for (var j = 0; j < instrs_received[storage_id].length;
						j++) {
						check_instr_fmt(instrs_received[storage_id][j].line);
					}
				}
				next(null, transformer, listeners, ctx, shard);
			}, DELAY);

		}, function (transformer, listeners, ctx, shard, next) {
			var received_keys = {};
			listeners.mako_listener.on('instruction', function (instr) {
				mod_assertplus.ok(false, 'received mako instruction for zero ' +
					'byte object');
			});

			setTimeout(function () {
				mod_assertplus.equal(num_records, Object.keys(received_keys).length,
					'did not receive all expected zero byte object keys');
				listeners.mako_listener.removeAllListeners('instruction');
				listeners.moray_listener.removeAllListeners('cleanup');
				next();
			}, DELAY);

			listeners.moray_listener.on('cleanup', function (key) {
				mod_assertplus.ok(TEST_ZERO_BYTE_OBJECT_KEYS.slice(0,
					num_records).indexOf(key) !== -1, 'received unexpected ' +
					'zero by object key');
				received_keys[key] = true;
			});

			TEST_ZERO_BYTE_OBJECT_RECORDS.slice(0, num_records).forEach(
				function (record) {
				transformer.emit('record', record);
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
		run_delete_record_transformer_test(NUM_TEST_RECORDS, next);
	},
	function (_, next) {
		/*
		 * Emit 1 fewer than the batch size. Make sure records still get flushed.
		 */
		run_delete_record_transformer_test(NUM_TEST_RECORDS - 1, next);
	}
]}, function (err) {
	console.log('tests passed');
	process.exit(0);
});



