/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

/*
 * Translates delete records intro instructions that can be uploaded to
 * Manta and interpreted by Makos.
 */

var mod_assertplus = require('assert-plus');
var mod_events = require('events');
var mod_util = require('util');

function
MorayDeleteRecordTransformer(opts)
{
	var self = this;

	mod_assertplus.object(opts, 'opts');
	mod_assertplus.object(opts.ctx, 'opts.ctx');
	mod_assertplus.object(opts.log, 'opts.log');
	mod_assertplus.object(opts.listener, 'opts.listener');

	self.mt_log = opts.log.child({
		component: 'MorayDeleteRecordTransformer'
	});
}
mod_util.inherits(MorayDeleteRecordTransformer, mod_events.EventEmitter);

module.exports = {

	MorayDeleteRecordTransformer: MorayDeleteRecordTransformer

};
