/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

/*
 * Receives transformed instructions from a transformer and uploads
 * them to Manta in a location that is known by Mako gc scripts.
 */
var mod_assertplus = require('assert-plus');
var mod_fsm = require('mooremachine');
var mod_util = require('util');

function
MakoInstructionUploader(opts)
{
	var self = this;

	mod_assertplus.object(opts, 'opts');
	mod_assertplus.object(opts.log, 'opts.log');
	mod_assertplus.object(opts.ctx, 'opts.ctx');
	mod_assertplus.object(opts.listener, 'opts.listener');

	self.mu_log = opts.log.child({
		component: 'MakoInstructionUploader'
	});

	mod_fsm.FSM.call(self, 'running');
}
mod_util.inherits(MakoInstructionUploader, mod_fsm.FSM);


MakoInstructionUploader.prototype.state_running = function
state_running(S)
{
};


module.exports = {

	MakoInstructionUploader: MakoInstructionUploader

};
