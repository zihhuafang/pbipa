BUILD_DIR?=build
ENABLED_TESTS?=true
export ENABLED_TESTS BUILD_DIR

.PHONY: all modules

all: modules/pancake/build/src/pancake modules/falconc/src/falconc modules/nighthawk/build/src/nighthawk modules/racon/build-meson/racon modules/pbmm2/build/src/pbmm2 modules/pb-layout/build/src/pblayout
	${MAKE} symlink

symlink:
	mkdir -p ${BUILD_DIR}/bin
	cd ${BUILD_DIR}/bin && ln -sf ../../modules/pancake/build/src/pancake
	cd ${BUILD_DIR}/bin && ln -sf ../../modules/falconc/src/falconc
	cd ${BUILD_DIR}/bin && ln -sf ../../modules/nighthawk/build/src/nighthawk
	cd ${BUILD_DIR}/bin && ln -sf ../../modules/pb-layout/build/src/pblayout
	cd ${BUILD_DIR}/bin && ln -sf ../../modules/racon/build-meson/racon
	cd ${BUILD_DIR}/bin && ln -sf ../../modules/pbmm2/build/src/pbmm2
	cd ${BUILD_DIR}/bin && ln -sf ../../bash/ipa2-task
	cd ${BUILD_DIR}/bin && ln -sf ../../scripts/ipa2_ovlp_to_graph
	cd ${BUILD_DIR}/bin && ln -sf ../../scripts/ipa2_graph_to_contig
	cd ${BUILD_DIR}/bin && ln -sf ../../scripts/ipa2_construct_config
	ls -larth ${BUILD_DIR}

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

modules/pb-layout/build/src/pblayout: modules/pb-layout/README.md
	cd modules/pb-layout && make conf build

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
