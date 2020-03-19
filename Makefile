CURRENT_BUILD_DIR?=build
IPA2_PREFIX?=LOCAL
ENABLED_TESTS?=true
export ENABLED_TESTS CURRENT_BUILD_DIR IPA2_PREFIX

.PHONY: all modules

all: modules/pancake/build/src/pancake modules/falconc/src/falconc modules/nighthawk/build/src/nighthawk modules/racon/build-meson/racon modules/pbmm2/build/src/pbmm2 modules/pb-layout/tmp/src/pblayout
	mkdir -p ${IPA2_PREFIX}/bin
	cd ${IPA2_PREFIX}/bin && ln -sf ../../modules/pancake/build/src/pancake
	cd ${IPA2_PREFIX}/bin && ln -sf ../../modules/falconc/src/falconc
	cd ${IPA2_PREFIX}/bin && ln -sf ../../modules/nighthawk/build/src/nighthawk
	cd ${IPA2_PREFIX}/bin && ln -sf ../../modules/pb-layout/tmp/src/pblayout
	cd ${IPA2_PREFIX}/bin && ln -sf ../../modules/racon/build-meson/racon
	cd ${IPA2_PREFIX}/bin && ln -sf ../../modules/pbmm2/build/src/pbmm2
	cd ${IPA2_PREFIX}/bin && ln -sf ../../bash/ipa2-task
	cd ${IPA2_PREFIX}/bin && ln -sf ../../scripts/ipa2_ovlp_to_graph
	cd ${IPA2_PREFIX}/bin && ln -sf ../../scripts/ipa2_graph_to_contig
	cd ${IPA2_PREFIX}/bin && ln -sf ../../scripts/ipa2_construct_config

modules/pancake/build/src/pancake: modules/pancake/README.md
	cd modules/pancake && make all

modules/falconc/src/falconc: modules/falconc/readme.md
	cd modules/falconc/ && make

modules/nighthawk/build/src/nighthawk: modules/nighthawk/README.md
	cd modules/nighthawk && mkdir -p build && cd build && (meson ../ || meson --reconfigure) && ninja

modules/racon/build-meson/racon: modules/racon/README.md
	cd modules/racon && make meson

modules/pbmm2/build/src/pbmm2: modules/pbmm2/README.md
	cd modules/pbmm2 && meson   --buildtype release --default-library shared --libdir lib --unity off --prefix /usr/local -Db_coverage=false -Db_sanitize=none -Dtests=false build . && ninja -C build -v

modules/pb-layout/tmp/src/pblayout: modules/pb-layout/README.md

###########################
### Update the modules. ###
###########################
modules/pancake/README.md: modules
modules/falconc/readme.md: modules
modules/nighthawk/README.md: modules
modules/racon/README.md: modules
modules/pbmm2/README.md: modules
modules/pb-layout/README.md: modules

modules:
	git submodule update --init --recursive --remote

# cram: modules
# 	scripts/cram tests/cram/*.t
