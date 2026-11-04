#!/usr/bin/env bash
# Runs one bounded IncrementalSync worker for one configured job key.

set -u

# shellcheck source=lib.sh
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

if [ "$#" -ne 1 ]; then
    echo "usage: klaviyo-incremental.sh JOB_KEY" >&2
    exit 2
fi

job_key="$1"
if ! klaviyo_is_configured_job "$job_key"; then
    echo "invalid Klaviyo job key" >&2
    exit 2
fi

java_bin="${KLAVIYO_JAVA_BIN:-java}"
classpath="${KLAVIYO_CLASSPATH:-$(klaviyo_default_classpath)}"

exec "$java_bin" \
    -classpath "$classpath" \
    --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED \
    com.greyfielddata.klaviyo.IncrementalSync \
    "$job_key"
