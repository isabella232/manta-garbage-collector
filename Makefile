#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright 2020 Joyent, Inc.
#

#
# Makefile: Manta Fast Garbage Collection System
#

NAME := manta-garbage-collector

NODE_PREBUILT_TAG = zone64
NODE_PREBUILT_VERSION = v6.17.0
NODE_PREBUILT_IMAGE = c2c31b00-1d60-11e9-9a77-ff9f06554b0f
NPM_ENV = NODE_ENV=production

PROTO = proto
PREFIX = /opt/smartdc/$(NAME)
ROOT := $(shell pwd)

CLEAN_FILES += $(PROTO)
ESLINT_FILES := $(shell find bin lib test -name '*.js')

RELEASE_TARBALL = $(NAME)-pkg-$(STAMP).tar.gz

ENGBLD_USE_BUILDIMAGE = true
ENGBLD_REQUIRE := $(shell git submodule update --init deps/eng)
include ./deps/eng/tools/mk/Makefile.defs
TOP ?= $(error Unable to access eng.git submodule Makefiles.)
include ./deps/eng/tools/mk/Makefile.node_prebuilt.defs
include ./deps/eng/tools/mk/Makefile.agent_prebuilt.defs
include ./deps/eng/tools/mk/Makefile.node_modules.defs
include ./deps/eng/tools/mk/Makefile.smf.defs

ifneq ($(shell uname -s),SunOS)
	NPM=npm
	NODE=node
	NPM_EXEC=$(shell which npm)
	NODE_EXEC=$(shell which node)
endif

#
# Stuff used for buildimage
#
# our base image is triton-origin-x86_64-18.4.0
BASE_IMAGE_UUID = a9368831-958e-432d-a031-f8ce6768d190
BUILDIMAGE_NAME = mantav2-garbage-collector
BUILDIMAGE_DESC = Manta Garbage Collector
BUILDIMAGE_PKGSRC = nginx-1.14.2 # used by tests to approximate mako
AGENTS = amon config registrar

.PHONY: all
all: $(STAMP_NODE_MODULES)
	$(NODE) --version

.PHONY: release
release: $(NODE_EXEC) $(STAMP_NODE_MODULES)
	@echo "==> Building $(RELEASE_TARBALL)"
	@$(ROOT)/build/node/bin/node ./node_modules/.bin/kthxbai
	mkdir -p $(PROTO)$(PREFIX)
	mkdir -p $(PROTO)$(PREFIX)/../boot
	cp -r $(ROOT)/bin \
	    $(ROOT)/lib \
	    $(ROOT)/node_modules \
	    $(ROOT)/package.json \
	    $(ROOT)/sapi_manifests \
	    $(ROOT)/smf \
	    $(ROOT)/test \
	    $(PROTO)$(PREFIX)/
	cp $(ROOT)/build/node/bin/node $(PROTO)$(PREFIX)/bin/
	chmod 755 $(PROTO)$(PREFIX)/bin/node
	mkdir -p $(PROTO)$(PREFIX)/scripts
	cp $(ROOT)/deps/manta-scripts/*.sh $(PROTO)$(PREFIX)/scripts/
	cp $(ROOT)/boot/*.sh $(PROTO)$(PREFIX)/scripts/
	chmod 755 $(PROTO)$(PREFIX)/scripts/*.sh
	(cd $(PROTO)$(PREFIX)/../boot && ln -s ../$(NAME)/scripts/{setup,configure}.sh .)
	cd $(PROTO) && gtar -I pigz -cf $(TOP)/$(RELEASE_TARBALL) \
	    --transform='s,^[^.],root/&,' \
	    --owner=0 --group=0 \
	    opt

.PHONY: publish
publish: release
	mkdir -p $(ENGBLD_BITS_DIR)/$(NAME)
	cp $(RELEASE_TARBALL) $(ENGBLD_BITS_DIR)/$(NAME)/$(RELEASE_TARBALL)

check:: $(NODE_EXEC)

# Just lint check (no style)
.PHONY: lint
lint: | $(ESLINT)
	$(ESLINT) --rule 'prettier/prettier: off' $(ESLINT_FILES)

.PHONY: fmt
fmt: | $(ESLINT)
	$(ESLINT) --fix $(ESLINT_FILES)

.PHONY: test
test:
	@echo "To run tests, run:"
	@echo ""
	@echo "    ./bin/node node_modules/.bin/tap test/*.test.js"
	@echo ""
	@echo "from the /opt/smartdc/manta-garbage-collector directory on a garbage-collector instance."


include ./deps/eng/tools/mk/Makefile.deps
include ./deps/eng/tools/mk/Makefile.targ
include ./deps/eng/tools/mk/Makefile.node_prebuilt.targ
include ./deps/eng/tools/mk/Makefile.agent_prebuilt.targ
include ./deps/eng/tools/mk/Makefile.node_modules.targ
include ./deps/eng/tools/mk/Makefile.smf.targ
