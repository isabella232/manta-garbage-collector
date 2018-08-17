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


var DELAY = 30000;
var TEST_OWNER_ONE = mod_uuidv4();
var TEST_OWNER_TWO = mod_uuidv4();

var TEST_INSTRUCTIONS = {
	'1.stor.orbit.example.com': [
		{
			line: [TEST_OWNER_ONE, mod_uuidv4()],
			key: mod_path.join(TEST_OWNER_ONE, mod_uuidv4()),
			cleaned_state: {
				cleaned: false
			}
		},
		{
			line: [TEST_OWNER_TWO, mod_uuidv4()],
			key: mod_path.join(TEST_OWNER_ONE, mod_uuidv4()),
			cleaned_state: {
				cleaned: false
			}

		}
	],
	'2.stor.orbit.example.com': [
		{
			line: [TEST_OWNER_ONE, mod_uuidv4()],
			key: mod_path.join(TEST_OWNER_TWO, mod_uuidv4()),
			cleaned_state: {
				cleaned: false
			}
		},
		{
			line: [TEST_OWNER_TWO, mod_uuidv4()],
			key: mod_path.join(TEST_OWNER_TWO, mod_uuidv4()),
			cleaned_state: {
				cleaned: false
			}
		}
	]
};

var TEST_EXPECTED_CLEANUP_KEYS = (function () {
	var keys = [];

	for (var i = 0; i < Object.keys(TEST_INSTRUCTIONS).length; i++) {
		var key = Object.keys(TEST_INSTRUCTIONS)[i];
		var value = TEST_INSTRUCTIONS[key];

		keys = keys.concat(value.map(function (instr) {
			return (instr.key);
		}));
	}
	return (keys);
})();


(function
main()
{

	mod_vasync.waterfall([
		function create_context(next) {
			lib_testcommon.create_mock_context(next);
		},
		function create_mako_instruction_uploader(ctx, next) {
			var listener = new mod_events.EventEmitter();
			var uploader = lib_testcommon.create_mako_instruction_uploader(ctx,
				listener);
			next(null, ctx, uploader, listener);
		},
		function emit_and_check_for_cleanups(ctx, uploader, listener, next) {
			var keys_received = {};
			listener.on('cleanup', function (keys) {
				mod_assertplus.arrayOfString(keys, 'keys');
				keys.forEach(function (key) {
					mod_assertplus.ok(TEST_EXPECTED_CLEANUP_KEYS.indexOf(key)
						!== -1, 'received cleanup request for ' +
						'unexpected key');
					mod_assertplus.ok(!keys_received.hasOwnProperty(key),
						'received the same key twice');
					keys_received[key] = true;
				});
			});

			mod_vasync.forEachParallel({
				inputs: Object.keys(TEST_INSTRUCTIONS),
				func: function emit(storage_id, done) {
					uploader.emit('instruction', {
						storage_id: storage_id,
						lines: TEST_INSTRUCTIONS[storage_id]
					});

					done();
				}
			}, function (err) {});

			setTimeout(function () {
				ctx.ctx_log.debug({
					received: keys_received
				}, 'received cleanup keys');
				ctx.ctx_log.debug({
					expected: TEST_EXPECTED_CLEANUP_KEYS
				}, 'expected cleanup keys');
				mod_assertplus.equal(Object.keys(keys_received).length,
					TEST_EXPECTED_CLEANUP_KEYS.length, 'did not ' +
					'received all cleanup keys');
				TEST_EXPECTED_CLEANUP_KEYS.forEach(function (key) {
					mod_assertplus.ok(keys_received.hasOwnProperty(key),
						'did not receive expected cleanup request');
				});
				listener.removeAllListeners('cleanup');
				next(null, ctx, uploader);
			}, DELAY);
		},
		function find_cleanup_instructions(ctx, uploader, next) {
			var client = ctx.ctx_manta_client;

			/*
			 * Keep track of all the instructions that have been
			 * uploaded to Manta.
			 */
			var instrs_found = {};

			function find_instr_in_manta(key, done) {
				var manta_storage_id = key;
				var prefix = ctx.ctx_mako_cfg.instr_upload_path_prefix;
				var path = mod_path.join(prefix, manta_storage_id);

				/*
				 * Collect all the objects from the path we
				 * uploaded instructions to. For each storage
				 * node, this will be
				 * /poseidon/stor/manta_gc/mako/1.stor.orbit.example.com
				 */
				function get_object_paths(cb) {
					ctx.ctx_log.debug('search for paths in ' + path);
					var objs = [];
					client.ls(path, {}, function (err, stream) {
						stream.on('object', function (obj) {
							objs.push(mod_path.join(path,
								obj.name));
						});

						stream.on('error', function (err) {
							console.log(err, 'unable to list ' +
								'mako instrs');
							cb(err);
						});

						stream.on('end', function () {
							cb(null, objs);
						});
					});
				}

				/*
				 * Get the contents of each instruction object
				 * uploaded.
				 */
				function get_object_data(paths, cb) {
					ctx.ctx_log.debug('search data in objects ' + paths);
					var chunks = [];
					function get_obj(path, _cb) {
						client.get(path, {}, function (err, stream) {
							var contents = '';
							if (err) {
								_cb(err);
								return;
							}
							stream.on('data', function (buf) {
								contents = contents.concat(
									buf.toString());
							});
							stream.on('end', function () {
								mod_assertplus.ok(contents,
									'inspecting empty object');
								ctx.ctx_log.debug('contents ' + contents);
								chunks.push(contents);
								_cb();
							});
							stream.on('error', function (err) {
								_cb(err);
							});
						});
					}

					mod_vasync.forEachParallel({
						inputs: paths,
						func: get_obj
					}, function (err) {
						cb(err, chunks);
					});
				}

				/*
				 * For each data chunk search for the string
				 * matching the expected output key.
				 */
				function search_object_data(chunks, cb) {
					function search(chunk, instr) {
						var substr = instr.line.join('\t');
						var key = instr.key;

						if (chunk.indexOf(substr) !== -1) {
							mod_assertplus.ok(!instrs_found.hasOwnProperty(key),
								'instruction uploaded multiple times');
							ctx.ctx_log.debug('found instruction for key '
								+ key);
							instrs_found[key] = true;
						}
					}

					function search_obj(chunk, _cb) {
						mod_vasync.forEachParallel({
							inputs: TEST_INSTRUCTIONS[manta_storage_id],
							func: function (instr, __cb) {
								search(chunk, instr);
								__cb();
							}
						}, function (err) {
							_cb();
						});
					}
					mod_vasync.forEachParallel({
						inputs: chunks,
					 	func: search_obj
					}, function (err) {
						cb(err);
					});
				}

				mod_vasync.waterfall([
					get_object_paths,
					get_object_data,
					search_object_data
				], function (err) {
					done(err);
				});
			}

			mod_vasync.forEachParallel({
				inputs: Object.keys(TEST_INSTRUCTIONS),
				func: find_instr_in_manta
			}, function (err) {
				/*
				 * Here we verify that each of the objects we
				 * were meant to upload instructions for we
				 * indeed found.
				 */
				ctx.ctx_log.debug({
					found: Object.keys(instrs_found)
				}, 'found instructions');
				ctx.ctx_log.debug({
					expected: TEST_EXPECTED_CLEANUP_KEYS
				}, 'expected instructions');

				var keys_found = Object.keys(instrs_found);

				TEST_EXPECTED_CLEANUP_KEYS.forEach(function (key) {
					var matched = keys_found.filter(function (_key) {
						return (key === _key);
					});
					mod_assertplus.ok(matched.length > 0, 'did ' +
						'not find instruction for expected key');
				});

				next();
			});
		}
	], function (err) {
		if (err) {
			process.exit(1);
		}
		console.log('tests passed');
		process.exit(0);
	});
})();
