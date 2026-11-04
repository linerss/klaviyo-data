#!/usr/bin/env bash
# One scheduled slice: run one worker, then check (and maybe advance) the cohort.
#
# Intended to run from cron every five minutes. Each run picks the next job
# key in the fair rotation from the current five-minute slot, so every stream
# gets regular slots without any per-stream schedule to maintain.

set -u

script_root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
. "$script_root/lib.sh"

lock_file="${KLAVIYO_CYCLE_LOCK_FILE:-${TMPDIR:-/tmp}/klaviyo-sync-cycle.lock}"
slot="${KLAVIYO_CYCLE_SLOT:-$(( $(date -u +%s) / 300 ))}"

if [[ ! "$slot" =~ ^[0-9]+$ ]]; then
    echo "KLAVIYO_CYCLE_CONFIG_ERROR invalid slot" >&2
    exit 2
fi

exec 9>"$lock_file"
if ! flock -w 30 9; then
    echo "KLAVIYO_CYCLE_BUSY"
    exit 75
fi

if [ -n "${KLAVIYO_JOB_KEY:-}" ]; then
    job_key="$KLAVIYO_JOB_KEY"
else
    if ! rotation_text="$(klaviyo_rotation)"; then
        echo "KLAVIYO_CYCLE_CONFIG_ERROR invalid KLAVIYO_EVENT_METRICS" >&2
        exit 2
    fi
    mapfile -t rotation <<< "$rotation_text"
    job_key="${rotation[$(( slot % ${#rotation[@]} ))]}"
fi

echo "KLAVIYO_CYCLE_START job=$job_key"
"$script_root/klaviyo-incremental.sh" "$job_key"
worker_exit=$?

if [ "${KLAVIYO_DEFER_COHORT_HEALTH:-false}" = true ] \
    && [ "$worker_exit" -eq 0 ]; then
    echo "KLAVIYO_CYCLE_HEALTH_DEFERRED job=$job_key"
    echo "KLAVIYO_CYCLE_END job=$job_key"
    exit 0
fi

java_bin="${KLAVIYO_JAVA_BIN:-java}"
classpath="${KLAVIYO_CLASSPATH:-$(klaviyo_default_classpath)}"
"$java_bin" \
    -classpath "$classpath" \
    --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED \
    com.greyfielddata.klaviyo.CohortHealth
health_exit=$?

if [ "$worker_exit" -ne 0 ] || [ "$health_exit" -ne 0 ]; then
    echo "KLAVIYO_CYCLE_FAIL job=$job_key worker_exit=$worker_exit health_exit=$health_exit"
    exit 1
fi

echo "KLAVIYO_CYCLE_END job=$job_key"
