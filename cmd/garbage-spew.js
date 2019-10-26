/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2019, Joyent, Inc.
 */

var assert = require('assert-plus');
var moray = require('moray');
var restifyClients = require('restify-clients');
var vasync = require('vasync');
var verror = require('verror');
var VError = verror.VError;

// XXX rename this common before I die
var common = require('../lib/common.new');


function GarbageSpewer(opts) {
    var self = this;

    // XXX assert params

    self.concurrency = 100;
    self.config = opts.config;
    self.generation = 0;
    self.log = opts.log.child({
        component: 'GarbageSpewer'
    });
    self.victim = '1.stor.cascadia.joyent.us';

    self.log.info({opts: opts}, 'new GarbageSpewer');

    // XXX can switch to https to test timeout (uses port 443 which isn't open)
    self.client = restifyClients.createStringClient({
        url: 'http://' + self.victim
    });
}

GarbageSpewer.prototype.start = function start(callback) {
    var self = this;

    var idx;
    var startTime = process.hrtime();
    var queue;

    self.log.info('starting');

    self.payload = '';

    for (idx = 0; idx < 200; idx++) {
        self.payload = self.payload + 'mako\t' + self.victim + '\tbaf3c911-06dc-4949-8b12-d287a4e185cc\t5dbeaa80-233a-494b-9e6c-6eb626718787\n';
    }

    function handler(generation, cb) {
        var beginning = process.hrtime();

        self.client.put('/manta_gc/trash.' + generation, self.payload, function(err, req, res, data) {
            var elapsed = common.elapsedSince(beginning);

            /*
            self.log.info({
                elapsed: elapsed,
                err: err,
                generation: generation,
                res: res,
                req: req

                //code: res.statusCode,
                //headers: res.headers,
            }, 'spewed trash');
            */

            cb(err);
        });
    }

    queue = vasync.queue(handler, self.concurrency);
    queue.on('end', function handleEnd() {
        var elapsed = common.elapsedSince(startTime);
        self.log.info({elapsed: elapsed}, 'total run complete');
        callback();
    });

    for (idx = 0; idx < 1e4; idx++) {
        queue.push(idx);
    }
    queue.close();

    self.log.info('started!');
};

function main() {
    var logger;

    vasync.pipeline({
        arg: {},
        funcs: [
            function _createLogger(ctx, cb) {
                logger = ctx.log = common.createLogger({
                    name: 'garbage-spewer'
                });

                cb();
            }, function _loadConfig(ctx, cb) {
                common.loadConfig({log: ctx.log}, function _onConfig(err, cfg) {
                    if (!err) {
                        ctx.config = cfg;
                    }
                    cb(err);
                });
            }, function _validateConfig(ctx, cb) {
                ctx.log.info({ctx: ctx}, 'would validate config');
                common.validateConfig(ctx.config, function _onValidated(err, res) {
                    // XXX wtf
                    cb(err);
                });
            }, function _createGarbageSpewer(ctx, cb) {
                var idx;
                var gs;

                for (idx = 0; idx < 1; idx++) {
                    gs = new GarbageSpewer({
                        config: ctx.config,
                        log: ctx.log
                    });
                    gs.start(cb);
                }
            }
        ]
    }, function _doneMain(err) {
        logger.info('startup complete');
    });
}

main();
