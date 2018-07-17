/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

var Ajv = require('ajv');
var mod_assertplus = require('assert-plus');
var mod_jsprim = require('jsprim');

var mod_errors = require('./errors');

var InvalidShardConfigError = mod_errors.InvalidShardConfigError;
var InvalidMakoConfigError = mod_errors.InvalidMakoConfigError;
var InvalidCreatorsConfigError = mod_errors.InvalidCreatorsConfigError;

var AJV_ENV = new Ajv();

var SORT_ATTRS = [
	'_mtime'
];

var SORT_ORDERS = [
	'DESC',
	'desc',
	'ASC',
	'asc'
];


var moray_cfg_properties = {
	'record_read_batch_size': {
		'type': 'integer',
		'minimum': 1
	},
	'record_read_wait_interval': {
		'type': 'integer',
		'minimum': 0
	},
	'record_read_sort_attr': {
		'type': 'string',
		'enum': SORT_ATTRS
	},
	'record_read_sort_order': {
		'type': 'string',
		'enum': SORT_ORDERS
	},
	'record_delete_batch_size': {
		'type': 'integer',
		'minimum': 1
	},
	'record_delete_delay': {
		'type': 'integer',
		'minimum': 0
	}
};
var moray_property_names = Object.keys(moray_cfg_properties);


var mako_cfg_properties = {
	'instr_upload_batch_size': {
		'type': 'integer',
		'minimum': 1
	},
	'instr_upload_flush_delay': {
		'type': 'integer',
		'minimum': 1
	},
	'instr_upload_path_prefix': {
		'type': 'string',
		'minLength': 1
	}
};
var mako_property_names = Object.keys(mako_cfg_properties);


var shard_cfg_properties = {
	'domain_suffix': {
		'type': 'string',
		'minLength': 1
	},
	'interval': {
		'type': 'array',
		'minItems': 2,
		'maxItems': 2,
		'items': {
			'type': 'integer',
			'minimum': 1
		}
	},
	'buckets': { allOf: [ { '$ref': 'bucket-cfg' } ] }
};
var shard_property_names = Object.keys(shard_cfg_properties);


var creator_cfg_properties = {
	'uuid': {
		'type': 'string',
		'format': 'uuid',
		'minLength': 1
	}
};
var creator_property_names = Object.keys(creator_cfg_properties);


AJV_ENV.addSchema({
	id: 'bucket-cfg',
	type: 'array',
	items: {
		type: 'object',
		properties: {
			'bucket': {
				type: 'string',
				minLength: 1
			},
			'concurrency': {
				type: 'integer',
				minimum: 0
			}
		},
		required: [
			'bucket',
			'concurrency'
		],
		additionalProperties: false
	}
});


AJV_ENV.addSchema({
	id: 'shards-cfg',
	type: 'object',
	properties: shard_cfg_properties,
	required: shard_property_names,
	additionalProperties: false
});


AJV_ENV.addSchema({
	id: 'creator-cfg',
	type: 'object',
	properties: creator_cfg_properties,
	require: creator_property_names,
	additionalProperties: false
});


AJV_ENV.addSchema({
	id: 'creators-cfg',
	type: 'array',
	items: { allOf: [ { '$ref': 'creator-cfg' } ] },
	minItems: 1
});


AJV_ENV.addSchema({
	id: 'update-moray-cfg',
	type: 'object',
	properties: mod_jsprim.mergeObjects(moray_cfg_properties,
		shard_cfg_properties),
	additionalProperties: false
});


AJV_ENV.addSchema({
	id: 'update-mako-cfg',
	type: 'object',
	properties: mako_cfg_properties,
	additionalProperties: false
});


AJV_ENV.addSchema({
	id: 'moray-cfg',
	type: 'object',
	properties: moray_cfg_properties,
	required: moray_property_names,
	additionalProperties: false
});


AJV_ENV.addSchema({
	id: 'mako-cfg',
	type: 'object',
	properties: mako_cfg_properties,
	required: mako_property_names,
	additionalProperties: false
});


// Internal helpers

function
allowed_values_text(allowed)
{
	mod_assertplus.array(allowed, 'allowed');

	var seen = {};
	var values = [];

	allowed.map(JSON.stringify).forEach(function (str) {
		var key = str.toUpperCase();
		if (!seen.hasOwnProperty(key)) {
			seen[key] = true;
			values.push(str);
		}
	});

	return (' (' + values.join(', ') + ')');
}


function
error_message(err, name)
{
	var msg = name + err.dataPath + ' ' + err.message;

	if (err.params.hasOwnProperty('allowedValues')) {
		msg += allowed_values_text(err.params.allowedValues);
	}

	return (msg);
}


function
errors_text(errs, name)
{
	return (errs.map(function (err) {
		return (error_message(err, name));
	}).join(', '));
}


// Schema validation API


function
validate_shards_cfg(cfg)
{
	if (AJV_ENV.validate('shards-cfg', cfg)) {
		if (cfg.interval[0] > cfg.interval[1]) {
			return (new InvalidShardConfigError(
				'invalid shard number interval'));
		}
		return (null);
	}
	return (new InvalidShardConfigError('%s',
		errors_text(AJV_ENV.errors, 'shards-cfg')));
}


function
validate_creators_cfg(cfg)
{
	if (AJV_ENV.validate('creators-cfg', cfg)) {
		return null;
	}

	return (new InvalidCreatorsConfigError('%s',
		errors_text(AJV_ENV.errors, 'creators-cfg')));
}


function
validate_moray_cfg(cfg)
{
	if (AJV_ENV.validate('moray-cfg', cfg)) {
		return (null);
	}

	return (new InvalidShardConfigError('%s',
		errors_text(AJV_ENV.errors, 'moray-cfg')));
}


function
validate_mako_cfg(cfg)
{
	if (AJV_ENV.validate('mako-cfg', cfg)) {
		return (null);
	}

	return (new InvalidMakoConfigError('%s',
		errors_text(AJV_ENV.errors, 'mako-cfg')));
}


function
validate_update_moray_cfg(cfg)
{
	if (AJV_ENV.validate('update-moray-cfg', cfg)) {
		return (null);
	}

	return (new InvalidShardConfigError('%s',
		errors_text(AJV_ENV.errors, 'update-moray-cfg')));
}


function
validate_update_mako_cfg(cfg)
{
	if (AJV_ENV.validate('update-mako-cfg', cfg)) {
		return (null);
	}

	return (new InvalidMakoConfigError('%s',
		errors_text(AJV_ENV.errors, 'update-mako-cfg')));
}


module.exports = {

	validate_shards_cfg: validate_shards_cfg,

	validate_creators_cfg: validate_creators_cfg,

	validate_moray_cfg: validate_moray_cfg,

	validate_update_moray_cfg: validate_update_moray_cfg,

	validate_mako_cfg: validate_mako_cfg,

	validate_update_mako_cfg: validate_update_mako_cfg

};
