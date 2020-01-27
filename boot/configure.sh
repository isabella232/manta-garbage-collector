#!/bin/bash
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright 2020 Joyent, Inc.
#

#
# Runs on every boot of a "garbage-collector" zone.
#
# (Installed as "configure.sh" which is the standard name, and will be executed
# by the "user-script")
#

printf '==> configure.sh @ %s\n' "$(date -u +%FT%TZ)"

exit 0

