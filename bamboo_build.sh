#!/usr/bin/env bash
set -e

################
# DEPENDENCIES #
################

if [[ $USER == bamboo ]]; then
  ## Load modules
  set +vx
  type module >& /dev/null || . /mnt/software/Modules/current/init/bash
  source module.sh
  set -vx
fi

export CC="ccache gcc"
export CXX="ccache g++"
export CCACHE_BASEDIR="${PWD}"

if [[ -z ${bamboo_planRepository_branchName+x} ]]; then
  : #pass
elif [[ ! -d /pbi/flash/bamboo/ccachedir ]]; then
  echo "[WARNING] /pbi/flash/bamboo/ccachedir is missing"
elif [[ $bamboo_planRepository_branchName == develop ]]; then
  export CCACHE_DIR=/pbi/flash/bamboo/ccachedir/${bamboo_shortPlanKey}.${bamboo_shortJobKey}.develop
  export CCACHE_TEMPDIR=/scratch/bamboo.ccache_tempdir
elif [[ $bamboo_planRepository_branchName == master ]]; then
  export CCACHE_DIR=/pbi/flash/bamboo/ccachedir/${bamboo_shortPlanKey}.${bamboo_shortJobKey}.master
  export CCACHE_TEMPDIR=/scratch/bamboo.ccache_tempdir
elif [[ $USER == bamboo ]]; then
  _shortPlanKey=$(echo ${bamboo_shortPlanKey}|sed -e 's/[0-9]*$//')
  export CCACHE_DIR=/pbi/flash/bamboo/ccachedir/${bamboo_shortPlanKey}.${bamboo_shortJobKey}
  if [[ -d /pbi/flash/bamboo/ccachedir/${_shortPlanKey}.${bamboo_shortJobKey}.develop ]]; then
    ( cd /pbi/flash/bamboo/ccachedir/
      cp -a ${_shortPlanKey}.${bamboo_shortJobKey}.develop $CCACHE_DIR
    )
  fi
  export CCACHE_TEMPDIR=/scratch/bamboo.ccache_tempdir
fi

case "${bamboo_planRepository_branchName}" in
  develop|master)
    export PREFIX_ARG="/mnt/software/p/pancake/${bamboo_planRepository_branchName}"
    export BUILD_NUMBER="${bamboo_globalBuildNumber:-0}"
    ;;
  *)
    export BUILD_NUMBER="0"
    ;;
esac


# call the main build+test scripts
export ENABLED_TESTS="true"
export ENABLED_INTERNAL_TESTS="${bamboo_ENABLED_INTERNAL_TESTS}"
export LDFLAGS="-static-libstdc++ -static-libgcc"

source env.sh
export PATH=${IPA2_WORKSPACE}/bash/:$PATH
which ipa2-task
cd examples/ivan-200k-t1
make

# if [[ -z ${PREFIX_ARG+x} ]]; then
#   echo "Not installing anything (branch: ${bamboo_planRepository_branchName}), exiting."
#   exit 0
# fi

# source scripts/ci/install.sh
