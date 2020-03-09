CURRENT_BUILD_DIR?=build
IPA2_PREFIX?=LOCAL
ENABLED_TESTS?=true
export ENABLED_TESTS CURRENT_BUILD_DIR IPA2_PREFIX

.PHONY: all modules

all: modules/pancake/build/src/pancake modules/falconc/src/falconc modules/nighthawk/build/src/nighthawk
	mkdir -p ${IPA2_PREFIX}/bin
	cd ${IPA2_PREFIX}/bin && ln -sf ../../modules/pancake/build/src/pancake
	cd ${IPA2_PREFIX}/bin && ln -sf ../../modules/falconc/src/falconc
	cd ${IPA2_PREFIX}/bin && ln -sf ../../modules/nighthawk/build/src/nighthawk
	cd ${IPA2_PREFIX}/bin && ln -sf ../../bash/ipa2-task

modules:
	git submodule update --init --recursive

modules/pancake/build/src/pancake: modules
	cd modules/pancake && make all

modules/falconc/src/falconc: modules
	cd modules/falconc/ && make

modules/nighthawk/build/src/nighthawk: modules
	cd modules/nighthawk && mkdir -p build && cd build && (meson ../ || meson --reconfigure) && ninja

# cram: modules
# 	scripts/cram tests/cram/*.t
