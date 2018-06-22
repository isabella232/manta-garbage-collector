/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

/*
 * Common inclusion point for all the actors in the GCWorker pipeline.
 */

var MorayDeleteRecordReader = require('./moray_delete_record_reader').MorayDeleteRecordReader;
var MorayDeleteRecordCleaner = require('./moray_delete_record_cleaner').MorayDeleteRecordCleaner;
var MorayDeleteRecordTransformer = require('./moray_delete_record_transformer').MorayDeleteRecordTransformer;
var MakoInstructionUploader = require('./mako_instruction_uploader').MakoInstructionUploader;

module.exports = {

	MorayDeleteRecordReader: MorayDeleteRecordReader,

	MorayDeleteRecordCleaner: MorayDeleteRecordCleaner,

	MorayDeleteRecordTransformer: MorayDeleteRecordTransformer,

	MakoInstructionUploader: MakoInstructionUploader

};
