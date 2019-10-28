#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright 2019 Joyent, Inc.
#

#
# Makefile: Manta Fast Garbage Collection System
#

NAME := mantav2v2-garbage-collector

NODE_PREBUILT_TAG = zone64
NODE_PREBUILT_VERSION = v6.17.0
NODE_PREBUILT_IMAGE = c2c31b00-1d60-11e9-9a77-ff9f06554b0f

PROTO = proto
PREFIX = /opt/smartdc/$(NAME)
ROOT := $(shell pwd)

CLEAN_FILES += $(PROTO)

RELEASE_TARBALL = $(NAME)-pkg-$(STAMP).tar.gz

ENGBLD_USE_BUILDIMAGE = true
ENGBLD_REQUIRE := $(shell git submodule update --init deps/eng)
include ./deps/eng/tools/mk/Makefile.defs
TOP ?= $(error Unable to access eng.git submodule Makefiles.)
include ./deps/eng/tools/mk/Makefile.node_prebuilt.defs
include ./deps/eng/tools/mk/Makefile.agent_prebuilt.defs
include ./deps/eng/tools/mk/Makefile.node_modules.defs
include ./deps/eng/tools/mk/Makefile.smf.defs

#
# Stuff used for buildimage
#
# our base image is triton-origin-x86_64-18.4.0
BASE_IMAGE_UUID = a9368831-958e-432d-a031-f8ce6768d190
BUILDIMAGE_NAME = $(NAME)
BUILDIMAGE_DESC = Manta Garbage Collector
AGENTS = amon config registrar

.PHONY: all
all: $(STAMP_NODE_PREBUILT) $(STAMP_NODE_MODULES) install
	$(NODE) --version

.PHONY: test
test:
	echo "success"

.PHONY: install
install: $(NODE_EXEC) $(STAMP_NODE_MODULES)
	mkdir -p $(PROTO)$(PREFIX)
	mkdir -p $(PROTO)$(PREFIX)/../boot
	cp -r $(ROOT)/lib \
	    $(ROOT)/build \
	    $(ROOT)/node_modules \
	    $(ROOT)/package.json \
	    $(ROOT)/sapi_manifests \
	    $(ROOT)/smf \
	    $(ROOT)/tools \
	    $(PROTO)$(PREFIX)/
	mkdir -p $(PROTO)$(PREFIX)/scripts
	cp deps/manta-scripts/*.sh $(PROTO)$(PREFIX)/scripts/
	cp scripts/*.sh $(PROTO)$(PREFIX)/scripts/
	(cd $(PROTO)$(PREFIX)/../boot && ln -s ../{setup,configure}.sh .)

.PHONY: release
release: install
	@echo "==> Building $(RELEASE_TARBALL)"
	cd $(PROTO) && gtar -I pigz -cf $(TOP)/$(RELEASE_TARBALL) \
	    --transform='s,^[^.],root/&,' \
	    --owner=0 --group=0 \
	    opt

.PHONY: publish
publish: release
	mkdir -p $(ENGBLD_BITS_DIR)/$(NAME)
	cp $(RELEASE_TARBALL) $(ENGBLD_BITS_DIR)/$(NAME)/$(RELEASE_TARBALL)

check:: $(NODE_EXEC)


include ./deps/eng/tools/mk/Makefile.deps
include ./deps/eng/tools/mk/Makefile.targ
include ./deps/eng/tools/mk/Makefile.node_prebuilt.targ
include ./deps/eng/tools/mk/Makefile.agent_prebuilt.targ
include ./deps/eng/tools/mk/Makefile.node_modules.targ
include ./deps/eng/tools/mk/Makefile.smf.targ
