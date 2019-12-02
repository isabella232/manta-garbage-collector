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

NAME :=				manta-garbage-collector
CATEST :=			deps/catest/catest

NODE_PREBUILT_TAG =		zone64
NODE_PREBUILT_VERSION =		v4.8.7
NODE_PREBUILT_IMAGE =		18b094b0-eb01-11e5-80c1-175dac7ddf02

NODE_DEV_SYMLINK =		node

PROTO =				proto
PREFIX =			/opt/smartdc/$(NAME)

CLEAN_FILES +=			$(PROTO)

RELEASE_TARBALL =		$(NAME)-pkg-$(STAMP).tar.gz

ENGBLD_USE_BUILDIMAGE =		true
ENGBLD_REQUIRE :=		$(shell git submodule update --init deps/eng)
include ./deps/eng/tools/mk/Makefile.defs
TOP ?= $(error Unable to access eng.git submodule Makefiles.)
include ./deps/eng/tools/mk/Makefile.node_prebuilt.defs
include ./deps/eng/tools/mk/Makefile.agent_prebuilt.defs
include ./deps/eng/tools/mk/Makefile.node_modules.defs
include ./deps/eng/tools/mk/Makefile.smf.defs

#
# Install macros and targets:
#

COMMANDS =			$(subst .js,,$(notdir $(wildcard cmd/*.js)))

LIB_FILES =			$(notdir $(wildcard lib/*.js))

SCRIPTS =			firstboot.sh \
				everyboot.sh \
				backup.sh \
				services.sh \
				util.sh
SCRIPTS_DIR =			$(PREFIX)/scripts

TEMPLATES =			$(notdir $(wildcard templates/*))
TEMPLATES_DIR =			$(PREFIX)/templates

BOOT_SCRIPTS =			setup.sh configure.sh
BOOT_DIR =			/opt/smartdc/boot

SAPI_MANIFESTS =		manta-garbage-collector
SAPI_MANIFEST_DIRS =		$(SAPI_MANIFESTS:%=$(PREFIX)/sapi_manifests/%)

SMF_MANIFESTS_FILES =		garbage-collector
SMF_MANIFESTS =			$(SMF_MANIFESTS_FILES:%=smf/manifests/%.xml)
SMF_MANIFESTS_DIR =		$(PREFIX)/smf/manifests

NODE_BITS =			bin/node
NODE_DIR =			$(PREFIX)/node
NODE_MODULE_INSTALL =		$(PREFIX)/node_modules/.ok

INSTALL_FILES =			$(addprefix $(PROTO), \
				$(BOOT_SCRIPTS:%=$(BOOT_DIR)/%) \
				$(SCRIPTS:%=$(SCRIPTS_DIR)/%) \
				$(TEMPLATES:%=$(TEMPLATES_DIR)/%) \
				$(SMF_MANIFESTS:%=$(PREFIX)/%) \
				$(NODE_BITS:%=$(NODE_DIR)/%) \
				$(NODE_MODULE_INSTALL) \
				$(COMMANDS:%=$(PREFIX)/cmd/%.js) \
				$(COMMANDS:%=$(PREFIX)/bin/%) \
				$(LIB_FILES:%=$(PREFIX)/lib/%) \
				$(PREFIX)/lib/wrap.sh \
				$(SAPI_MANIFEST_DIRS:%=%/template) \
				$(SAPI_MANIFEST_DIRS:%=%/manifest.json) \
				$(PREFIX)/bin/garbage-collector \
				)

INSTALL_DIRS =			$(addprefix $(PROTO), \
				$(SCRIPTS_DIR) \
				$(TEMPLATES_DIR) \
				$(SMF_MANIFESTS_DIR) \
				$(BOOT_DIR) \
				$(NODE_DIR)/bin \
				$(NODE_DIR)/lib \
				$(PREFIX)/cmd \
				$(PREFIX)/bin \
				$(PREFIX)/lib \
				$(SAPI_MANIFEST_DIRS) \
				)

INSTALL_EXEC =			rm -f $@ && cp $< $@ && chmod 755 $@
INSTALL_FILE =			rm -f $@ && cp $< $@ && chmod 644 $@

CATEST_FILES = 			$(shell find test -name '*.test.js')

JS_FILES =			$(shell find cmd -name '*.js') \
				$(shell find lib -name '*.js') \
				$(shell find tools -name '*.js')

BASH_FILES =			$(shell find tools -name "assign_shards_to_collectors.sh")

JSL_FILES_NODE = 		$(JS_FILES)

JSSTYLE_FILES = 		$(JS_FILES)

JSL_CONF_NODE = 		deps/eng/tools/jsl.node.conf

JSON_FILES = 			package.json

#
# Stuff used for buildimage
#
BASE_IMAGE_UUID		= 04a48d7d-6bb5-4e83-8c3b-e60a99e0f48f
BUILDIMAGE_NAME		= mantav1-garbage-collector
BUILDIMAGE_DESC		= Manta Garbage Collector
AGENTS = amon config registrar

.PHONY: all
all: $(STAMP_NODE_PREBUILT) $(STAMP_NODE_MODULES) install
	$(NODE) --version

.PHONY: test
test: | $(CATEST)
	$(CATEST) $(CATEST_FILES)

$(CATEST): deps/catest/.git

$(INSTALL_FILES): manta-scripts

.PHONY: install
install: $(NODE_EXEC) $(INSTALL_FILES)

$(INSTALL_DIRS):
	mkdir -p $@

manta-scripts: ./deps/manta-scripts/.git

$(PROTO)$(BOOT_DIR)/setup.sh: | $(INSTALL_DIRS)
	rm -f $@ && ln -s ../$(NAME)/scripts/firstboot.sh $@

$(PROTO)$(BOOT_DIR)/configure.sh: | $(INSTALL_DIRS)
	rm -f $@ && ln -s ../$(NAME)/scripts/everyboot.sh $@

$(PROTO)$(PREFIX)/scripts/%.sh: deps/manta-scripts/%.sh | $(INSTALL_DIRS)
	$(INSTALL_EXEC)

$(PROTO)$(PREFIX)/scripts/%.sh: boot/%.sh | $(INSTALL_DIRS)
	$(INSTALL_EXEC)

$(PROTO)$(PREFIX)/templates/%: templates/% | $(INSTALL_DIRS)
	$(INSTALL_FILE)

$(PROTO)$(PREFIX)/node/bin/%: $(INSTALL_DIRS)
	rm -f $@ && cp $(NODE_INSTALL)/bin/$(@F) $@ && chmod 755 $@

$(PROTO)$(PREFIX)/node/lib/%: $(INSTALL_DIRS)
	rm -f $@ && cp $(NODE_INSTALL)/lib/$(@F) $@ && chmod 755 $@

$(PROTO)$(PREFIX)/cmd/%.js: cmd/%.js | $(INSTALL_DIRS)
	$(INSTALL_FILE)

$(PROTO)$(PREFIX)/bin/%:
	rm -f $@ && ln -s ../lib/wrap.sh $@

$(PROTO)$(PREFIX)/bin/garbage-collector: scripts/garbage-collector.sh
	$(INSTALL_EXEC)

$(PROTO)$(PREFIX)/lib/%.sh: lib/%.sh | $(INSTALL_DIRS)
	$(INSTALL_EXEC)

$(PROTO)$(PREFIX)/lib/%.js: lib/%.js | $(INSTALL_DIRS)
	$(INSTALL_FILE)

$(PROTO)$(NODE_MODULE_INSTALL): $(STAMP_NODE_MODULES) | $(INSTALL_DIRS)
	rm -rf $(@D)/
	cp -rP node_modules/ $(@D)/
	touch $@

$(PROTO)$(PREFIX)/sapi_manifests/%: sapi_manifests/% | $(INSTALL_DIRS)
	$(INSTALL_FILE)

$(PROTO)$(PREFIX)/smf/manifests/%.xml: smf/manifests/%.xml | $(INSTALL_DIRS)
	$(INSTALL_FILE)


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
