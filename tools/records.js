/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var mod_cmdutil = require('cmdutil');
var mod_manta = require('manta');
var mod_moray = require('moray');
var mod_getopt = require('posix-getopt');
var mod_util = require('util');
var mod_uuidv4 = require('uuid/v4');
var mod_vasync = require('vasync');

var lib_testcommon = require('../test/common');

var create_record = lib_testcommon.create_fake_delete_record;


function
random()
{
	return Math.floor(Math.random());
}

var TEST_OWNER = mod_uuidv4();
var TEST_SHARKS = [
	'1.stor.orbit.example.com',
	'2.stor.orbit.example.com'
];
var TEST_NO_SHARKS = [];

var OPTS = {
	'-b': 'bucket (default: manta_fastdelete_queue)',
	'-c': 'concurrency (default: 1)',
	'-d': 'ms delay delay (default: 1000)'
};

/* construct the usage message */
var usageMessage = '    OPTIONS';
Object.keys(OPTS).forEach(function (arg) {
	        usageMessage += mod_util.format('\n\t%s\t%s', arg, OPTS[arg]);
});


function
main()
{
	var options;

	var config = {
		bucket: lib_testcommon.MANTA_FASTDELETE_QUEUE,
		concurrency: 1,
		delay: 1000
	};

	mod_cmdutil.configure({
		'synopses': ['[OPTIONS]'],
		'usageMessage': usageMessage
	});
	mod_cmdutil.exitOnEpipe();

	var parser = new mod_getopt.BasicParser('b:(bucket)c:(concurrency)d:(delay)',
		process.argv);

	while ((option = parser.getopt()) !== undefined) {
		switch (option.option) {
			case 'b':
				config.bucket = option.optarg;
				break;
			case 'c':
				config.concurrency = mod_jsprim.parseInteger(
						option.optarg);
				break;
			case 'd':
				config.delay = mod_jsprim.parseInteger(
						option.optarg);
				break;
			default:
				mod_assertplus.equal('?', option.option);
				mod_cmdutil.usage();
				break;
		}
	}

	mod_vasync.waterfall([
		function init(next) {
			lib_testcommon.create_mock_context(next);
		},
		function start(ctx, next) {
			var shard = Object.keys(ctx.ctx_moray_clients)[0];
			var client = ctx.ctx_moray_clients[shard];

			function upload_record() {
				/*
				 * Randomly choose between zero byte and non-zero byte
				 * objects.
				 */
				var sharks = random() == 0 ? TEST_SHARKS : TEST_NO_SHARKS;
				create_record(ctx, client, config.bucket, TEST_OWNER,
					mod_uuidv4(), sharks, function (err) {
					if (err) {
						ctx.ctx_log.error(err, 'failed to upload ' +
							'record');
					} else {
						ctx.ctx_log.info('successfully uploaded ' +
							'record');
					}

					setTimeout(upload_record, config.delay);
				});
			}

			for (var i = 0; i < config.concurrency; i++) {
				setImmediate(upload_record);
			}
		}
	], function (err) {
		if (err) {
			process.exit(1);
		}
	});
}

main();
