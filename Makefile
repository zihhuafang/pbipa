CURRENT_BUILD_DIR?=build
PLATINUM_PREFIX?=LOCAL
ENABLED_TESTS?=true
export ENABLED_TESTS CURRENT_BUILD_DIR PLATINUM_PREFIX

.PHONY: all modules

all: modules/pancake/build/src/pancake modules/falconc/src/falconc
	mkdir -p ${PLATINUM_PREFIX}/bin
	cd ${PLATINUM_PREFIX}/bin && ln -sf ../../modules/pancake/build/src/pancake
	cd ${PLATINUM_PREFIX}/bin && ln -sf ../../modules/falconc/src/falconc
	cd ${PLATINUM_PREFIX}/bin && ln -sf ../../bash/platinum-task

modules:
	git submodule update --init --recursive

modules/pancake/build/src/pancake: modules
	cd modules/pancake && make all

modules/falconc/src/falconc: modules
	cd modules/falconc/ && make

# cram: modules
# 	scripts/cram tests/cram/*.t
