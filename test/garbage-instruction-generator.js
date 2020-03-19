/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

/*
 * This program generates dummy garbage collector instruction files for test
 * purposes.
 *
 * Run like:
 *
 *   ./bin/node test/garbage-instruction-generator.js 50000 1.stor.cascadia.joyent.us /tmp
 *
 * which would generate 50000 instructions in files (200 instructions per file)
 * named /tmp/*.instruction and destined for the mako 1.stor.cascadia.joyent.us.
 *
 */

var fs = require('fs');
var path = require('path');

var uuidv4 = require('uuid/v4');

var CHUNK_SIZE = 200;
var SHARD = '42.moray.test.joyent.us';

function _instrFilename(stor) {
    var date = new Date()
        .toISOString()
        .replace(/[-:]/g, '')
        .replace(/\..*$/, 'Z');

    // This matches what we're using in the consumers
    return (
        [date, uuidv4(), 'X', uuidv4(), 'mako-' + stor].join('-') +
        '.instruction'
    );
}

function usage(msg) {
    if (msg) {
        console.error(msg);
    }
    console.error('Usage: ' + process.argv[1] + ' <count> <stor> <dir>');
}

function main() {
    var chunk;
    var chunkSize = CHUNK_SIZE;
    var count = 0;
    var data;
    var directory = process.argv[4];
    var filename;
    var idx;
    var stor = process.argv[3];
    var targetCount = process.argv[2];

    if (!targetCount || !stor || !directory || process.argv[5]) {
        usage('Bad number of arguments.');
        process.exit(1);
    }

    if (!targetCount.match(/^[0-9]+$/)) {
        usage('Invalid count: ' + targetCount);
        process.exit(1);
    }

    if (!stor.match(/^[0-9]+\.stor\./)) {
        usage('Invalid stor: ' + stor);
        process.exit(1);
    }

    if (!fs.existsSync(directory)) {
        usage('Directory does not exist: ' + directory);
        process.exit(1);
    }

    while (count < targetCount) {
        if (targetCount - count > chunkSize) {
            chunk = chunkSize;
        } else {
            chunk = targetCount - count;
        }

        filename = _instrFilename(stor);

        data = '';
        for (idx = 0; idx < chunk; idx++) {
            data +=
                [
                    stor,
                    uuidv4(),
                    uuidv4(),
                    SHARD,
                    Math.ceil(Math.random() * 10000000)
                ].join('\t') + '\n';
        }

        fs.writeFileSync(path.join(directory, filename), data);
        console.log(
            'wrote ' +
                chunk +
                ' instructions to ' +
                path.join(directory, filename)
        );

        count += chunk;
    }
}

// Kick off the party!

main();
