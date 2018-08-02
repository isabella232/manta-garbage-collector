/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var mod_assertplus = require('assert-plus');
var mod_sdc = require('sdc-clients');
var mod_verror = require('verror');

var VE = mod_verror.VError;


function
get_sapi_application(log, sapi, name, include_master, callback)
{
	mod_assertplus.object(log, 'log');
	mod_assertplus.object(sapi, 'sapi');
	mod_assertplus.string(name, 'name');
	mod_assertplus.bool(include_master, 'include_master');
	mod_assertplus.func(callback, 'callback');

	var opts = {
		name: name
	};

	if (include_master) {
		opts.include_master = true;
	}

	sapi.listApplications(opts, function (err, apps) {
		if (err) {
			callback(new VE(err, 'Unable to get "%s" SAPI ' +
				'application', name));
			return;
		}
		if (apps.length !== 1) {
			callback(new VE(err, 'Found %d "%s" applications, ' +
				'wanted 1', apps.length, name));
			return;
		}

		callback(null, apps[0]);
	});
}


module.exports = {

	get_sapi_application: get_sapi_application

};
