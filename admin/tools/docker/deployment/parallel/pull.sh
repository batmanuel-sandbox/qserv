#!/bin/bash

# Download Qserv master and workers containers on all cluster nodes

# @author  Fabrice Jammes, IN2P3

set -e

DIR=$(cd "$(dirname "$0")"; pwd -P)

. "${DIR}/env.sh"

ssh "$MASTER" "docker pull ${MASTER_IMAGE}"
shmux -B -S all -c "docker pull ${WORKER_IMAGE}" $WORKERS
