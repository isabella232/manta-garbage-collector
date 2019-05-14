/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2019, Joyent, Inc.
 */

/*
 * Common inclusion point for all the actors in the GCWorker pipeline.
 */

var mod_reader = require('./moray_delete_record_reader');
var mod_cleaner = require('./moray_delete_record_cleaner');
var mod_transformer = require('./delete_record_transformer');
var mod_writer = require('./mako_instruction_writer');

module.exports = {

	MorayDeleteRecordReader: mod_reader.MorayDeleteRecordReader,

	MorayDeleteRecordCleaner: mod_cleaner.MorayDeleteRecordCleaner,

	DeleteRecordTransformer: mod_transformer.DeleteRecordTransformer,

	MakoInstructionWriter: mod_writer.MakoInstructionWriter

};
