/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var mod_assertplus = require('assert-plus');
var mod_util = require('util');
var mod_verror = require('verror');

function
InvalidShardsConfigError()
{
	mod_verror.WError.apply(this, arguments);
	this.name = this.constructor.name;
}
mod_util.inherits(InvalidShardsConfigError, mod_verror.WError);


function
InvalidTunablesConfigError()
{
	mod_verror.WError.apply(this, arguments);
	this.name = this.constructor.name;
}
mod_util.inherits(InvalidTunablesConfigError, mod_verror.WError);


function
InvalidCreatorsConfigError()
{
	mod_verror.WError.apply(this, arguments);
	this.name = this.constructor.name;
}
mod_util.inherits(InvalidCreatorsConfigError, mod_verror.WError);


function
InvalidBucketsConfigError()
{
	mod_verror.WError.apply(this, arguments);
	this.name = this.constructor.name;
}
mod_util.inherits(InvalidBucketsConfigError, mod_verror.WError);


module.exports = {

	InvalidShardsConfigError: InvalidShardsConfigError,

	InvalidBucketsConfigError: InvalidBucketsConfigError,

	InvalidCreatorsConfigError: InvalidCreatorsConfigError,

	InvalidTunablesConfigError: InvalidTunablesConfigError

};
