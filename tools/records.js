/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2019, Joyent, Inc.
 */

var mod_assertplus = require('assert-plus');
var mod_cmdutil = require('cmdutil');
var mod_jsprim = require('jsprim');
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
	return (Math.floor(Math.random()));
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
	'-d': 'ms delay delay (default: 1000)',
	'-a': 'account uuid',
	'-z': 'zero byte objects only',
	'-s': 'non-zero-byte objects only'
};

/* construct the usage message */
var usageMessage = '    OPTIONS';
Object.keys(OPTS).forEach(function (arg) {
	        usageMessage += mod_util.format('\n\t%s\t%s', arg, OPTS[arg]);
});


function
main()
{
	var option;

	var config = {
		bucket: lib_testcommon.MANTA_FASTDELETE_QUEUE,
		concurrency: 1,
		delay: 1000,
		account: TEST_OWNER
	};

	mod_cmdutil.configure({
		'synopses': ['[OPTIONS]'],
		'usageMessage': usageMessage
	});
	mod_cmdutil.exitOnEpipe();

	var parser = new mod_getopt.BasicParser('a:(account)b:(bucket)' +
		'c:(concurrency)d:(delay)z(zero-byte)s(storage)',
		process.argv);

	while ((option = parser.getopt()) !== undefined) {
		switch (option.option) {
			case 'a':
				mod_assertplus.uuid(option.optarg);
				config.account = option.optarg;
				break;
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
			case 's':
				config.storage = true;
				break;
			case 'z':
				config.zero_byte = true;
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
				var sharks;

				if ((config.storage && config.zero_byte) ||
				    !(config.storage || config.zero_byte)) {
					sharks = random() === 0 ? TEST_SHARKS :
						TEST_NO_SHARKS;

				} else if (config.storage) {
					sharks = TEST_SHARKS;
				} else {
					sharks = TEST_NO_SHARKS;
				}

				create_record(ctx, client, config.bucket,
					config.account, mod_uuidv4(), sharks,
					function (err) {
					setTimeout(upload_record,
						config.delay);
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
