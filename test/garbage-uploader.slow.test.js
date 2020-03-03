/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

/*
 * This file contains tests for the `garbage-uploader` which require actual
 * filesystem calls and talking to an nginx instance (which we'll create).
 *
 * Ideas for further testing:
 *
 *  - Test that mako being down and coming back does the right thing
 *  - Test that adding new mako to collection works
 *  - Test what happens when unlink fails after sending instruction file
 *  - Test all known failure modes for PUT, and ensure handled as expected
 *  - Test how many instructions we can PUT per minute?
 *  - Test what happens when we're out of space (locally and remotely)
 *
 */
var child_process = require('child_process');
var EventEmitter = require('events');
var fs = require('fs');
var path = require('path');

var assert = require('assert-plus');
var test = require('@smaller/tap').test;
var uuidv4 = require('uuid/v4');

var GarbageUploader = require('../lib/garbage-uploader.js');

var STOR_SUFFIX = '.stor.test.joyent.us';
var TEST_DIR = path.join('/tmp', _randomString() + '.garbage-uploader-test');
var TEST_DIR_INSTRUCTIONS = path.join(TEST_DIR, 'instructions');

var NGINXES = {
    nginx0: {
        mantaPath: path.join(TEST_DIR, 'nginx0', 'manta'),
        path: path.join(TEST_DIR, 'nginx0'),
        port: 10080,
        stor: 0
    },
    nginx1: {
        mantaPath: path.join(TEST_DIR, 'nginx1', 'manta'),
        path: path.join(TEST_DIR, 'nginx1'),
        port: 10081,
        stor: 1
    },
    nginx2: {
        mantaPath: path.join(TEST_DIR, 'nginx2', 'manta'),
        path: path.join(TEST_DIR, 'nginx2'),
        port: 10082,
        stor: 2
    },
    nginx3: {
        fake: true, // this one won't actually exist
        mantaPath: path.join(TEST_DIR, 'nginx3', 'manta'),
        path: path.join(TEST_DIR, 'nginx3'),
        port: 10083,
        stor: 3
    }
};

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
        logs.debug.push(Array.from(arguments));
    },
    error: function _error() {
        logs.error.push(Array.from(arguments));
    },
    info: function _info() {
        logs.info.push(Array.from(arguments));
    },
    trace: function _trace() {
        logs.trace.push(Array.from(arguments));
    },
    warn: function _warn() {
        logs.warn.push(Array.from(arguments));
    }
};
var test_dirs_created = false;
var uploader;
var uploadEmitter = new EventEmitter();

function _createNginxConfig(nginx) {
    assert.object(NGINXES[nginx], 'NGINXES[nginx]');

    var config = NGINXES[nginx];

    var baseDir = config.path;
    var filename = path.join(baseDir, 'nginx.conf');
    var lines;
    var listenPort = config.port;
    var mantaDir = config.mantaPath;

    lines = [
        'daemon off;',
        'worker_processes  8;',
        '',
        'error_log ' + baseDir + '/mako-error.log info;',
        '',
        'events {',
        '        worker_connections  1024;',
        '        use eventport;',
        '}',
        '',
        'http {',
        "        log_format      combined_withlatency '$remote_addr - $remote_user [$time_local] '",
        '            \'"$request" $status $body_bytes_sent $request_time \'',
        '            \'"$http_referer" "$http_user_agent"\';',
        '        access_log      ' +
            baseDir +
            '/mako-access.log combined_withlatency;',
        '        client_max_body_size 0;',
        '        default_type    application/octet-stream;',
        '        include         /opt/local/etc/nginx/mime.types;',
        '',
        '        keepalive_timeout 86400;',
        '        keepalive_requests 1000000;',
        '',
        '        sendfile        on;',
        '        send_timeout    300s;',
        '',
        '        server {',
        '                listen          ' +
            listenPort +
            ' so_keepalive=10s:30s:10;',
        '                root            ' + mantaDir + ';',
        '                server_name     localhost;',
        '',
        '                location /',
        '                {',
        '                        client_body_temp_path   ' +
            mantaDir +
            '/nginx_temp;',
        '                        create_full_put_path    on;',
        '                        dav_access              user:rw  group:r  all:r;',
        '                        dav_methods             PUT MOVE;',
        '                        expires                 max;',
        '                }',
        '',
        '                error_page 500 502 503 504 /50x.html;',
        '',
        '                location = /50x.html {',
        '                        root   html;',
        '                }',
        '        }',
        '}',
        ''
    ];

    fs.writeFileSync(filename, lines.join('\n'), {encoding: 'utf8'});
}

function _createTestDirs(t) {
    var dir;
    var dirs = [
        TEST_DIR,
        TEST_DIR_INSTRUCTIONS,
        NGINXES.nginx0.path,
        NGINXES.nginx0.mantaPath,
        path.join(NGINXES.nginx0.mantaPath, 'manta_gc'),
        path.join(TEST_DIR_INSTRUCTIONS, NGINXES.nginx0.stor + STOR_SUFFIX),
        path.join(
            TEST_DIR_INSTRUCTIONS,
            NGINXES.nginx0.stor + STOR_SUFFIX + '.tmp'
        ),
        NGINXES.nginx1.path,
        NGINXES.nginx1.mantaPath,
        path.join(NGINXES.nginx1.mantaPath, 'manta_gc'),
        path.join(TEST_DIR_INSTRUCTIONS, NGINXES.nginx1.stor + STOR_SUFFIX),
        path.join(
            TEST_DIR_INSTRUCTIONS,
            NGINXES.nginx1.stor + STOR_SUFFIX + '.tmp'
        ),
        NGINXES.nginx2.path,
        NGINXES.nginx2.mantaPath,
        path.join(NGINXES.nginx2.mantaPath, 'manta_gc'),
        path.join(TEST_DIR_INSTRUCTIONS, NGINXES.nginx2.stor + STOR_SUFFIX),
        path.join(
            TEST_DIR_INSTRUCTIONS,
            NGINXES.nginx2.stor + STOR_SUFFIX + '.tmp'
        )
    ];
    var i;

    for (i = 0; i < dirs.length; i++) {
        dir = dirs[i];
        fs.mkdirSync(dir);
        // eslint-disable-next-line no-octal
        fs.chmodSync(dir, 0777);
        t.ok(true, 'create ' + dir);
    }

    test_dirs_created = true;
}

function _deleteTestDirs(t) {
    if (test_dirs_created) {
        child_process.execFileSync('/usr/bin/rm', ['-fr', TEST_DIR], {});
        t.ok(true, 'delete ' + TEST_DIR);
    } else {
        t.ok(true, 'did not create test dirs that need deletion');
    }
}

function _randomString() {
    return Math.random()
        .toString(36)
        .slice(2);
}

function _putFileHook(obj) {
    uploadEmitter.emit('upload', obj);
}

function _runHook(obj) {
    uploadEmitter.emit('run', obj);
}

function _instrFilename(storageIdNum) {
    var date = new Date()
        .toISOString()
        .replace(/[-:]/g, '')
        .replace(/\..*$/, 'Z');

    // This matches what we're using in the consumers
    return (
        [
            date,
            uuidv4(),
            'X',
            uuidv4(),
            'mako-' + storageIdNum + STOR_SUFFIX
        ].join('-') + '.instruction'
    );
}

function _writeInstruction(nginx, data, opts, callback) {
    var dir = path.join(TEST_DIR_INSTRUCTIONS, nginx.stor + STOR_SUFFIX);
    var filename = _instrFilename(nginx.stor);
    var tmpDir = dir + '.tmp';

    if (opts && opts.filenameSuffix) {
        filename += opts.filenameSuffix;
    }

    fs.writeFile(path.join(tmpDir, filename), data, function _onWrite(err) {
        if (err) {
            callback(err);
            return;
        }

        fs.rename(
            path.join(tmpDir, filename),
            path.join(dir, filename),
            function _onRename(e) {
                if (e) {
                    callback(e);
                    return;
                }

                callback(null, filename);
            }
        );
    });
}

// setup

test('create testdirs', function _testCreateTestdirs(t) {
    t.doesNotThrow(function _callCreator() {
        _createTestDirs(t);
    }, 'create test directories');
    t.end();
});

test('start nginxes', function _testStartNginxes(t) {
    var idx = 0;
    var key;
    var keys;
    var nginx;

    keys = Object.keys(NGINXES);
    for (idx = 0; idx < keys.length; idx++) {
        key = keys[idx];
        nginx = NGINXES[key];

        if (nginx.fake) {
            continue;
        }

        _createNginxConfig(key);

        nginx.child = child_process.spawn('/opt/local/sbin/nginx', [
            '-c',
            path.join(nginx.path, 'nginx.conf')
        ]);

        t.ok(
            nginx.child && nginx.child.pid,
            'started ' +
                key +
                ' on port ' +
                nginx.port +
                ' with pid ' +
                nginx.child.pid
        );

        nginx.child.stderr.on('data', function _onStderr(data) {
            console.error('nginx STDERR: ' + data);
        });

        // eslint-disable-next-line no-loop-func
        nginx.child.on('close', function _onClose(code) {
            console.error('child process ' + key + ' exited with code ' + code);
        });
    }

    t.end();
});

test('create GarbageUploader', function _testCreateUploader(t) {
    var idx;
    var keys;
    var nginx;
    var storageServerMap = {};

    keys = Object.keys(NGINXES);
    for (idx = 0; idx < keys.length; idx++) {
        nginx = NGINXES[keys[idx]];
        storageServerMap[nginx.stor + STOR_SUFFIX] = '127.0.0.1:' + nginx.port;
    }

    uploader = new GarbageUploader({
        instructionRoot: TEST_DIR_INSTRUCTIONS,
        log: logger,
        _putFileHook: _putFileHook,
        runFreq: 100,
        _runHook: _runHook,
        storageServerMap: storageServerMap
    });

    t.ok(uploader, 'create GarbageUploader');

    uploader.start(function _started() {
        t.ok(true, 'start GarbageUploader');
        t.end();
    });
});

// test meat

test('upload single instruction for 0.stor', function _testUploadSingleInstruction0(t) {
    var writtenFilename;
    var inputData = 'hello';
    var nginx = NGINXES.nginx0;

    uploadEmitter.once('upload', function _onUpload(obj) {
        var outputData;

        t.equal(
            writtenFilename,
            obj.filename,
            'file uploaded should match what we sent'
        );
        outputData = fs.readFileSync(
            path.join(nginx.mantaPath, 'manta_gc/instructions', writtenFilename)
        );
        t.equal(
            inputData,
            outputData.toString(),
            'data should match what we uploaded'
        );
        t.throws(function _tryStat() {
            // eslint-disable-next-line no-unused-vars
            var stat = fs.statSync(
                path.join(
                    TEST_DIR_INSTRUCTIONS,
                    nginx.stor + STOR_SUFFIX,
                    obj.filename
                )
            );
        }, 'instruction file should have been deleted');

        t.end();
    });

    _writeInstruction(nginx, inputData, {}, function _onWrite(err, filename) {
        t.ifError(err, 'should have written instruction file');
        writtenFilename = filename;
    });
});

test('drop invalid instruction for 0.stor', function _testInvalidInstruction0(t) {
    var writtenFilename;
    var inputData = 'hello';
    var nginx = NGINXES.nginx0;

    t.equal(
        uploader.metrics.instrFileInvalidCountTotal,
        0,
        'should be no invalid instructions yet'
    );

    uploadEmitter.on('run', function _onRun(obj) {
        //
        // We might get a 'run' before we've actually written our file since
        // we're running the hot loop pretty hot. We'll ignore until we've
        // actually written the file.
        //
        if (writtenFilename) {
            uploadEmitter.removeAllListeners('run');

            t.equal(obj.instrsInvalid, 1, 'invalid instruction is invalid');
            t.ok(
                uploader.metrics.instrFileInvalidCountTotal > 0,
                'invalid file metric should also have been bumped'
            );

            t.end();
        }
    });

    _writeInstruction(
        nginx,
        inputData,
        {
            filenameSuffix: '.broken'
        },
        function _onWrite(err, filename) {
            writtenFilename = filename;
            t.ifError(err, 'should have written instruction file');
        }
    );
});

test('upload single instruction for 1.stor', function _testUploadSingleInstruction1(t) {
    var writtenFilename;
    var inputData = 'world';
    var nginx = NGINXES.nginx1;

    uploadEmitter.once('upload', function _onUpload(obj) {
        var outputData;

        t.equal(
            writtenFilename,
            obj.filename,
            'file uploaded should match what we sent'
        );
        outputData = fs.readFileSync(
            path.join(nginx.mantaPath, 'manta_gc/instructions', writtenFilename)
        );
        t.equal(
            inputData,
            outputData.toString(),
            'data should match what we uploaded'
        );
        t.throws(function _tryStat() {
            // eslint-disable-next-line no-unused-vars
            var stat = fs.statSync(
                path.join(
                    TEST_DIR_INSTRUCTIONS,
                    nginx.stor + STOR_SUFFIX,
                    obj.filename
                )
            );
        }, 'instruction file should have been deleted');

        t.end();
    });

    _writeInstruction(nginx, inputData, {}, function _onWrite(err, filename) {
        t.ifError(err, 'should have written instruction file');
        writtenFilename = filename;
    });
});

//
// This tests with an nginx that's pstopped so not responding. This should
// result in a timeout.
//
test('upload single instruction for 2.stor with nginx pstopped', function _testUploadSingleInstruction2Stopped(t) {
    var inputData = 'calculator';
    var nginx = NGINXES.nginx2;

    setTimeout(function _waitForNginx() {
        child_process.execFile(
            '/usr/bin/ptree',
            [nginx.child.pid],
            function _onPtree(err, stdout, stderr) {
                var processes = [];

                t.ifError(err, 'ptree should succeed');
                t.equal(stderr, '', 'stderr should be empty');

                // Find all the nginx processes (inc children) we just created so we
                // can make sure to stop them later.
                processes = stdout
                    .split('\n')
                    .map(function _foo(a) {
                        if (a.match(/sbin\/nginx/)) {
                            return a.trim().split(/\s+/)[0];
                        }
                        return undefined;
                    })
                    .filter(function _foo(a) {
                        return Boolean(a);
                    });

                // Stop the nginx processes so they can't respond to requests.
                child_process.execFile(
                    '/usr/bin/pstop',
                    processes,
                    function _onPstop(pstopErr) {
                        t.ifError(pstopErr, 'pstop should succeed');

                        if (pstopErr) {
                            t.end();
                            return;
                        }

                        uploadEmitter.once('upload', function _onUpload(obj) {
                            var filename = path.join(
                                TEST_DIR_INSTRUCTIONS,
                                nginx.stor + STOR_SUFFIX,
                                obj.filename
                            );
                            var stat;

                            t.ok(
                                obj.err &&
                                    obj.err.message.match(/failed to complete/),
                                'Should have failed with a timeout'
                            );
                            stat = fs.statSync(filename);
                            t.ok(stat, 'File should not have been deleted');

                            // Now delete it since we're done with this test.
                            fs.unlinkSync(filename);

                            // prun again so they can die when we try to kill below.
                            child_process.execFile(
                                '/usr/bin/prun',
                                processes,
                                function _onPrun(prunErr) {
                                    t.ifError(prunErr, 'prun should succeed');
                                    t.end();
                                }
                            );
                        });

                        _writeInstruction(
                            nginx,
                            inputData,
                            {},
                            function _onWrite(writeErr, filename) {
                                t.ifError(
                                    writeErr,
                                    'should have written instruction file: ' +
                                        filename
                                );
                            }
                        );
                    }
                );
            }
        );
    }, 1000);
});

// teardown / final checks

test('check metrics', function _testMetrics(t) {
    // This just checks that the metrics were set to some approximately
    // reasonable values.

    var metrics = uploader.metrics;

    function tGreater(metric, compare) {
        if (typeof(compare) === 'number') {
            t.ok(metrics[metric] > compare, metric + '(' + metrics[metric] +
                ') > ' + compare);
        } else if (typeof(compare) === 'string') {
            t.ok(metrics[metric] > metrics[compare], metric +
                '(' + metrics[metric] + ') > ' + compare + '(' +
                metrics[compare] + ')');
        } else {
            t.ok(false, 'bad tGreater type: ' + typeof(compare));
        }
    }

    tGreater('instrFileInvalidCountTotal', 0);
    tGreater('instrFileUploadAttemptCountTotal', 2);
    tGreater('instrFileUploadErrorCountTotal', 0);
    tGreater('instrFileUploadSecondsTotal', 0);
    tGreater('instrFileUploadSuccessCountTotal', 0);
    tGreater('instrFileDeleteCountTotal', 0);
    t.equal(metrics.instrFileDeleteErrorCountTotal, 0,
        'expected 0 delete errors');
    tGreater('watchedDirectoryCount', 2);
    tGreater('runCountTotal', 3);
    t.equal(metrics.runErrorCountTotal, 0, 'expected no errors running');
    tGreater('runSecondsTotal', 0);

    t.end();
});

test('stop GarbageUploader', function _testStopUploader(t) {
    uploader.stop(function _onStop(err) {
        t.error(err, 'stop GarbageUploader');
        t.end();
    });
});

test('delete testdirs', function _testDeleteTestdirs(t) {
    t.doesNotThrow(function _callDeleter() {
        _deleteTestDirs(t);
    }, 'delete test directories');
    t.end();
});

test('killing nginxes', function _testKillNginxes(t) {
    var idx;
    var key;
    var keys;
    var nginx;

    keys = Object.keys(NGINXES);
    for (idx = 0; idx < keys.length; idx++) {
        key = keys[idx];
        nginx = NGINXES[key];

        if (nginx.child) {
            t.ok(true, 'killing ' + key);
            nginx.child.kill();
        }
    }

    t.end();
});

test('dump logs', function _testDumpLogs(t) {
    var enabled = Boolean(process.env.DUMP_LOGS);

    t.ok(
        true,
        'log dump (env DUMP_LOGS is' + (!enabled ? ' not' : '') + ' set)',
        {skip: !enabled}
    );

    if (process.env.DUMP_LOGS) {
        console.dir(logs);
    }

    t.end();
});
