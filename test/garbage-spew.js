/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

/*
 * This program takes a directory and a target server and starts a
 * GarbageUploader that will upload all those instructions to a mako. It will
 * output some statistics about how the processing went.
 *
 */

var EventEmitter = require('events');
var fs = require('fs');

var GarbageUploader = require('../lib/garbage-uploader.js');

var logs = {
    debug: [],
    error: [],
    info: [],
    trace: [],
    warn: []
};
var logger = {
    child: function _child() {
        return logger;
    },
    debug: function _debug() {
        while (logs.debug.length > 100) {
            logs.debug.pop();
        }
        logs.debug.push(Array.from(arguments));
    },
    error: function _error() {
        while (logs.error.length > 100) {
            logs.error.pop();
        }
        logs.error.push(Array.from(arguments));
    },
    info: function _info() {
        while (logs.info.length > 100) {
            logs.info.pop();
        }
        logs.info.push(Array.from(arguments));
    },
    trace: function _trace() {
        while (logs.trace.length > 100) {
            logs.trace.pop();
        }
        logs.trace.push(Array.from(arguments));
    },
    warn: function _warn() {
        while (logs.warn.length > 100) {
            logs.warn.pop();
        }
        logs.warn.push(Array.from(arguments));
    }
};

var uploadEmitter = new EventEmitter();

function _putFileHook(obj) {
    uploadEmitter.emit('upload', obj);
}

function _runHook(obj) {
    uploadEmitter.emit('run', obj);
}

function usage(msg) {
    if (msg) {
        console.error(msg);
    }
    console.error('Usage: ' + process.argv[1] + ' <stor> <ip:port> <dir>');
}

function main() {
    var storageServerMap = {};
    var uploader;

    var directory = process.argv[4];
    var server = process.argv[3];
    var stor = process.argv[2];

    if (!stor || !server || !directory || process.argv[5]) {
        usage('Bad number of arguments.');
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

    storageServerMap[stor] = server;

    uploader = new GarbageUploader({
        instructionRoot: directory,
        log: logger,
        _putFileHook: _putFileHook,
        runFreq: 86400000, // don't run again today
        _runHook: _runHook,
        storageServerMap: storageServerMap
    });

    uploadEmitter.once('run', function _runComplete(obj) {
        console.log('Results:');
        console.dir(obj);
        // We finished one run now we want to stop the uploader. Since we
        // assume all files were in the dir before we started, we'll now
        // just look through the results of this one run.
        uploader.stop(function _onStop() {
            console.log('GarbageUploader stopped');
        });
    });

    uploader.start(function _started() {
        console.log('GarbageUploader started');
    });
}

main();
