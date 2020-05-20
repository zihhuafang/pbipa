#!/bin/bash
type module >& /dev/null || . /mnt/software/Modules/current/init/bash
module purge
module load git
module load gcc
module load ccache
module load meson
module load ninja
module load boost # no libs
module load zlib # static lib
module load htslib # static lib

# Substitute for static versions of our dependencies.
export PKG_CONFIG_LIBDIR=${PKG_CONFIG_LIBDIR//pkgconfig/pkgconfig-static}

# for testing
module load gtest cram samtools

echo "PKG_CONFIG_LIBDIR=${PKG_CONFIG_LIBDIR}"

set -vex
which gcc
gcc --version

ls -larth modules/*

# Get sub-repos, recursively.

#make modules
git submodule update --jobs 4 --init --recursive --remote


export LDFLAGS="-static-libstdc++ -static-libgcc"
#export CURRENT_BUILD_DIR=${PWD}/build-pbipa # nah, build in each subproject
export PREFIX_ARG=${PWD}/INSTALL
export LIB_TYPE=static
mkdir -p ${PREFIX_ARG} ${CURRENT_BUILD_DIR}

#rm -rf modules/*/build-meson

#(cd modules/falconc/ && make)

(mkdir -p modules/pancake/build-meson && cd modules/pancake/build-meson && (meson --default-library static --libdir lib --unity off --buildtype=release -Dc_args=-O3 -Dtests=true -Dtests-internal=false --prefix ${PREFIX_ARG} || meson --reconfigure) && ninja install -v)

(mkdir -p modules/nighthawk/build-meson && cd modules/nighthawk/build-meson && (meson --default-library static --buildtype=release -Dc_args=-O3 --prefix ${PREFIX_ARG} || meson --reconfigure) && ninja install -v)

(mkdir -p modules/pb-layout/build-meson && cd modules/pb-layout/build-meson && (meson --default-library static --buildtype=release -Dc_args=-O3 --prefix ${PREFIX_ARG} -Db_coverage=false -Db_sanitize=none -Dtests=false || meson --reconfigure) && ninja install -v)

#(mkdir -p modules/racon/build-meson && cd modules/racon/build-meson && (meson --default-library static --buildtype=release -Dc_args=-O3 --prefix ${PREFIX_ARG} || meson --reconfigure) && ninja install -v)

#(mkdir -p modules/pbmm2/build-meson && cd modules/pbmm2/build-meson && (meson --default-library static --buildtype=release -Dc_args=-O3 --prefix ${PREFIX_ARG} || meson --reconfigure) && ninja install -v)

rm -rf pbipa/

mkdir -p pbipa/bin

cp -f ${PREFIX_ARG}/bin/nighthawk pbipa/bin/
cp -f ${PREFIX_ARG}/bin/pancake pbipa/bin/
cp -f ${PREFIX_ARG}/bin/pblayout pbipa/bin/

ldd pbipa/bin/*
ls -larth pbipa/bin
strip pbipa/bin/*
ls -larth pbipa/bin

cp -fL bash/ipa2-task pbipa/bin/
cp -fL scripts/ipa pbipa/bin/
cp -fL scripts/ipa2_ovlp_to_graph pbipa/bin/
cp -fL scripts/ipa2_graph_to_contig pbipa/bin/

mkdir -p pbipa/etc
cp -fL etc/ipa.snakefile pbipa/etc/

find pbipa/

perl -pi -e "s/COMMIT=.*/COMMIT=' (commit ${REV})/" pbipa/bin/ipa2-task

tar cvfz pbipa.tar.gz pbipa
