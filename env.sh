DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

IPA2_WORKSPACE=${DIR}
IPA2_PREFIX=${DIR}/LOCAL
PYTHONUSERBASE=${IPA2_PREFIX}

PATH=${IPA2_PREFIX}/bin:${PATH}
LD_LIBRARY_PATH=${IPA2_PREFIX}/lib64:${LD_LIBRARY_PATH}

export PATH
export LD_LIBRARY_PATH
export LOCAL
export PYTHONUSERBASE
export IPA2_WORKSPACE
export IPA2_PREFIX
