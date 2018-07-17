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
InvalidShardConfigError()
{
	mod_verror.WError.apply(this, arguments);
	this.name = this.constructor.name;
}
mod_util.inherits(InvalidShardConfigError, mod_verror.WError);


function
InvalidMakoConfigError()
{
	mod_verror.WError.apply(this, arguments);
	this.name = this.constructor.name;
}
mod_util.inherits(InvalidMakoConfigError, mod_verror.WError);


function
InvalidCreatorsConfigError()
{
	mod_verror.WError.apply(this, arguments);
	this.name = this.constructor.name;
}
mod_util.inherits(InvalidCreatorsConfigError, mod_verror.WError);


module.exports = {

	InvalidShardConfigError: InvalidShardConfigError,

	InvalidMakoConfigError: InvalidMakoConfigError,

	InvalidCreatorsConfigError: InvalidCreatorsConfigError

};
