#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright 2019 Joyent, Inc.
#

#
# Makefile: basic Makefile for template API service
#

NAME :=				manta-garbage-collector

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

# This Makefile is a template for new repos. It contains only repo-specific
# logic and uses included makefiles to supply common targets (javascriptlint,
# jsstyle, restdown, etc.), which are used by other repos as well. You may well
# need to rewrite most of this file, but you shouldn't need to touch the
# included makefiles.
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

SMF_MANIFESTS_FILES =		garbage-dir-consumer garbage-uploader
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
				$(PREFIX)/etc/rsyncd.conf \
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
				$(PREFIX)/etc \
				$(SAPI_MANIFEST_DIRS) \
				)

INSTALL_EXEC =			rm -f $@ && cp $< $@ && chmod 755 $@
INSTALL_FILE =			rm -f $@ && cp $< $@ && chmod 644 $@


NODE_PREBUILT_VERSION=v6.17.0
ifeq ($(shell uname -s),SunOS)
        NODE_PREBUILT_TAG=zone64
        # TODO: should update this to a newer image too
        NODE_PREBUILT_IMAGE=18b094b0-eb01-11e5-80c1-175dac7ddf02
endif

ENGBLD_USE_BUILDIMAGE	= true
ENGBLD_REQUIRE		:= $(shell git submodule update --init deps/eng)
include ./deps/eng/tools/mk/Makefile.defs
TOP ?= $(error Unable to access eng.git submodule Makefiles.)

ifeq ($(shell uname -s),SunOS)
	include ./deps/eng/tools/mk/Makefile.node_prebuilt.defs
	include ./deps/eng/tools/mk/Makefile.agent_prebuilt.defs
endif
include ./deps/eng/tools/mk/Makefile.smf.defs

# Mountain Gorilla-spec'd versioning.
ROOT                    := $(shell pwd)
RELEASE_TARBALL         := $(NAME)-pkg-$(STAMP).tar.gz
RELSTAGEDIR                  := /tmp/$(NAME)-$(STAMP)
MGCSTAGEDIR                  := $(RELSTAGEDIR)/root/opt/smartdc/manta-garbage-collector

# our base image is triton-origin-x86_64-18.4.0
BASE_IMAGE_UUID = a9368831-958e-432d-a031-f8ce6768d190
BUILDIMAGE_NAME		= mantav2-garbage-collector
BUILDIMAGE_DESC	= Manta Garbage Collector
AGENTS		= amon config registrar
PATH	:= $(NODE_INSTALL)/bin:/opt/local/bin:${PATH}

#
# Repo-specific targets
#
.PHONY: all
all: $(SMF_MANIFESTS) | $(TAP) manta-scripts
	$(NPM) install

$(TAP): | $(NPM_EXEC)
	$(NPM) install

$(PROTO)$(PREFIX)/node/lib/%: $(INSTALL_DIRS)
	rm -f $@ && cp $(NODE_INSTALL)/lib/$(@F) $@ && chmod 755 $@

$(PROTO)$(PREFIX)/cmd/%.js: cmd/%.js | $(INSTALL_DIRS)
	$(INSTALL_FILE)

$(PROTO)$(PREFIX)/bin/%:
	rm -f $@ && ln -s ../lib/wrap.sh $@

$(PROTO)$(PREFIX)/etc/rsyncd.conf: etc/rsyncd.conf
	$(INSTALL_FILE)

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
release: check all docs
	@echo "Building $(RELEASE_TARBALL)"
	@mkdir -p $(MGCSTAGEDIR)/etc
	@mkdir -p $(RELSTAGEDIR)/site
	@touch $(RELSTAGEDIR)/site/.do-not-delete-me
	@mkdir -p $(RELSTAGEDIR)/root
	cp -r $(ROOT)/build \
		$(ROOT)/cmd \
		$(ROOT)/docs \
		$(ROOT)/lib \
		$(ROOT)/node_modules \
		$(ROOT)/README.md \
		$(ROOT)/package.json \
		$(ROOT)/sapi_manifests \
		$(ROOT)/smf \
		$(ROOT)/test \
		$(MGCSTAGEDIR)/
	mkdir -p $(MGCSTAGEDIR)/scripts
	cp -R $(ROOT)/deps/manta-scripts/*.sh $(MGCSTAGEDIR)/scripts/
	cp -R $(ROOT)/boot/*.sh $(MGCSTAGEDIR)/scripts/
	chmod 755 $(MGCSTAGEDIR)/scripts/*.sh
	mkdir -p $(RELSTAGEDIR)/root/opt/smartdc/boot
	rm -f $(RELSTAGEDIR)/root/opt/smartdc/boot/{configure,setup}.sh
	(cd $(RELSTAGEDIR) && $(TAR) -I pigz -cf $(ROOT)/$(RELEASE_TARBALL) root site)
	@rm -rf $(RELSTAGEDIR)


.PHONY: publish
publish: release
	@if [[ -z "$(ENGBLD_BITS_DIR)" ]]; then \
	  echo "error: 'ENGBLD_BITS_DIR' must be set for 'publish' target"; \
	  exit 1; \
	fi
	mkdir -p $(ENGBLD_BITS_DIR)/$(NAME)
	cp $(ROOT)/$(RELEASE_TARBALL) $(ENGBLD_BITS_DIR)/$(NAME)/$(RELEASE_TARBALL)


.PHONY: test
test: $(TAP)
	TAP=1 $(TAP) test/*.test.js


include ./deps/eng/tools/mk/Makefile.deps
ifeq ($(shell uname -s),SunOS)
	include ./deps/eng/tools/mk/Makefile.node_prebuilt.targ
	include ./deps/eng/tools/mk/Makefile.agent_prebuilt.targ
endif
include ./deps/eng/tools/mk/Makefile.smf.targ
include ./deps/eng/tools/mk/Makefile.targ
include ./deps/eng/tools/mk/Makefile.node_prebuilt.targ
include ./deps/eng/tools/mk/Makefile.agent_prebuilt.targ
include ./deps/eng/tools/mk/Makefile.node_modules.targ
