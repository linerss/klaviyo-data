#!/usr/bin/env bash
# Catch-up controller: runs back-to-back sync-cycle slices in the fair
# rotation until a fresh cohort completes or the time/iteration budget ends.
#
# Use it after an outage or for the initial backfill. Every slice keeps the
# worker's transactional checkpoints, so stopping at any point is safe and the
# next run continues where this one stopped.
#
# Exit codes: 0 complete or paused at budget, 75 another run holds the lock,
# 2 configuration error, otherwise the failing slice's exit code.
# A small JSON latch in $KLAVIYO_STATUS_DIR records failed or incomplete runs.

set -u
set -o pipefail

script_root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
. "$script_root/lib.sh"

status_dir="${KLAVIYO_STATUS_DIR:-$(klaviyo_project_root)/state}"
run_lock_file="${KLAVIYO_RUN_LOCK_FILE:-$status_dir/klaviyo-run.lock}"
flock_bin="${KLAVIYO_FLOCK_BIN:-flock}"
date_bin="${KLAVIYO_DATE_BIN:-date}"
sleep_bin="${KLAVIYO_SLEEP_BIN:-sleep}"
timeout_bin="${KLAVIYO_TIMEOUT_BIN:-timeout}"
python_bin="${KLAVIYO_PYTHON_BIN:-python3}"
cycle="${KLAVIYO_CYCLE_SCRIPT:-$script_root/klaviyo-sync-cycle.sh}"

max_seconds="${KLAVIYO_CATCHUP_MAX_SECONDS:-43200}"
max_iterations="${KLAVIYO_CATCHUP_MAX_ITERATIONS:-100000}"
sleep_seconds="${KLAVIYO_CATCHUP_SLEEP_SECONDS:-5}"
slice_seconds="${KLAVIYO_CATCHUP_SLICE_SECONDS:-230}"
lock_wait_seconds="${KLAVIYO_CATCHUP_LOCK_WAIT_SECONDS:-300}"
max_completed_cutoff_age_seconds="${KLAVIYO_CATCHUP_MAX_COMPLETED_CUTOFF_AGE_SECONDS:-3600}"
failure_latch="$status_dir/klaviyo-catchup-failure.json"
failure_latch_lock="$status_dir/.klaviyo-catchup-failure.lock"

validate_integer() {
    name="$1"
    value="$2"
    minimum="$3"
    maximum="$4"
    if [[ ! "$value" =~ ^[0-9]+$ ]] \
        || [ "$value" -lt "$minimum" ] \
        || [ "$value" -gt "$maximum" ]; then
        echo "invalid $name: expected $minimum-$maximum" >&2
        exit 2
    fi
}

validate_integer KLAVIYO_CATCHUP_MAX_SECONDS "$max_seconds" 600 86400
validate_integer KLAVIYO_CATCHUP_MAX_ITERATIONS "$max_iterations" 1 1000000
validate_integer KLAVIYO_CATCHUP_SLEEP_SECONDS "$sleep_seconds" 0 300
validate_integer KLAVIYO_CATCHUP_SLICE_SECONDS "$slice_seconds" 60 3600
validate_integer KLAVIYO_CATCHUP_LOCK_WAIT_SECONDS "$lock_wait_seconds" 0 600
validate_integer \
    KLAVIYO_CATCHUP_MAX_COMPLETED_CUTOFF_AGE_SECONDS \
    "$max_completed_cutoff_age_seconds" \
    300 \
    21600

if ! rotation_text="$(klaviyo_rotation)"; then
    echo "KLAVIYO_CATCHUP_CONFIG_ERROR invalid KLAVIYO_EVENT_METRICS" >&2
    exit 2
fi
mapfile -t jobs <<< "$rotation_text"
last_job_index=$((${#jobs[@]} - 1))

if [ ! -x "$cycle" ]; then
    echo "KLAVIYO_CATCHUP_CONFIG_ERROR missing executable cycle script" >&2
    exit 2
fi
if ! mkdir -p "$status_dir" || ! chmod 700 "$status_dir"; then
    echo "KLAVIYO_CATCHUP_CONFIG_ERROR could not prepare status directory" >&2
    exit 2
fi
exec 9>>"$failure_latch_lock"
chmod 600 "$failure_latch_lock"

exec 8>"$run_lock_file"
if ! "$flock_bin" -w "$lock_wait_seconds" 8; then
    echo "KLAVIYO_CATCHUP_BUSY"
    exit 75
fi

started_epoch="$("$date_bin" -u +%s)"
deadline_epoch=$((started_epoch + max_seconds))
iteration=0

write_catchup_latch() {
    latch_status="$1"
    latch_exit="$2"
    latch_reason="$3"
    finished_epoch="$("$date_bin" -u +%s)"
    failure_id="$started_epoch-$finished_epoch-$$-$latch_exit"
    tmp_latch="$status_dir/.klaviyo-catchup-failure.$$.tmp"
    if ! "$flock_bin" -x 9; then
        echo "KLAVIYO_CATCHUP_LATCH_ERROR could not lock failure latch" >&2
        return 1
    fi
    # An unresolved failure is never overwritten by a mere "incomplete".
    if [ "$latch_status" = incomplete ] \
        && [ -f "$failure_latch" ] \
        && ! grep -Eq \
            '"status"[[:space:]]*:[[:space:]]*"incomplete"' \
            "$failure_latch"; then
        echo "KLAVIYO_CATCHUP_LATCH_PRESERVED preserving unresolved latch"
        "$flock_bin" -u 9 || true
        return 10
    fi
    if ! printf \
        '{"label":"klaviyo-catchup","status":"%s","failure_id":"%s","exit_code":%s,"started_epoch":%s,"finished_epoch":%s,"reason":"%s"}\n' \
        "$latch_status" \
        "$failure_id" \
        "$latch_exit" \
        "$started_epoch" \
        "$finished_epoch" \
        "$latch_reason" \
        > "$tmp_latch"; then
        echo "KLAVIYO_CATCHUP_LATCH_ERROR could not write failure latch" >&2
        "$flock_bin" -u 9 || true
        return 1
    fi
    if ! chmod 600 "$tmp_latch" || ! mv "$tmp_latch" "$failure_latch"; then
        rm -f "$tmp_latch"
        echo "KLAVIYO_CATCHUP_LATCH_ERROR could not publish failure latch" >&2
        "$flock_bin" -u 9 || true
        return 1
    fi
    "$flock_bin" -u 9 || true
}

write_failure_latch() {
    write_catchup_latch failed "$1" "$2"
}

write_incomplete_latch() {
    write_catchup_latch incomplete 0 "$1"
}

handle_signal() {
    signal_exit="$1"
    trap - INT TERM
    write_failure_latch "$signal_exit" controller_signal || true
    exit "$signal_exit"
}

trap 'handle_signal 130' INT
trap 'handle_signal 143' TERM

echo "KLAVIYO_CATCHUP_START started_epoch=$started_epoch max_seconds=$max_seconds"

while true; do
    for job_index in "${!jobs[@]}"; do
        job_key="${jobs[$job_index]}"
        # Cohort health runs once per full rotation, after every stream has
        # had a slice, instead of after every slice.
        defer_cohort_health=true
        if [ "$job_index" -eq "$last_job_index" ]; then
            defer_cohort_health=false
        fi
        now_epoch="$("$date_bin" -u +%s)"
        if [ "$now_epoch" -ge "$deadline_epoch" ] \
            || [ "$iteration" -ge "$max_iterations" ]; then
            echo "KLAVIYO_CATCHUP_INCOMPLETE iterations=$iteration reason=budget_reached"
            write_incomplete_latch budget_reached
            latch_exit=$?
            if [ "$latch_exit" -ne 0 ] && [ "$latch_exit" -ne 10 ]; then
                exit 2
            fi
            exit 0
        fi

        iteration=$((iteration + 1))
        echo "KLAVIYO_CATCHUP_SLICE iteration=$iteration job=$job_key"
        slice_output="$(
            KLAVIYO_JOB_KEY="$job_key" \
                KLAVIYO_DEFER_COHORT_HEALTH="$defer_cohort_health" \
                "$timeout_bin" --kill-after=30 "$slice_seconds" "$cycle" \
                2>&1
        )"
        slice_exit=$?
        printf '%s\n' "$slice_output"

        if [ "$slice_exit" -ne 0 ]; then
            echo "KLAVIYO_CATCHUP_FAIL iteration=$iteration job=$job_key exit_code=$slice_exit"
            write_failure_latch "$slice_exit" slice_failed || true
            exit "$slice_exit"
        fi

        completed_cutoff="$(
            printf '%s\n' "$slice_output" \
                | sed -n 's/.*KLAVIYO_COHORT_COMPLETE cutoff=\([^[:space:]]*\).*/\1/p' \
                | tail -n 1
        )"
        if [ -n "$completed_cutoff" ]; then
            completed_epoch="$(
                "$python_bin" -c \
                    'import datetime,sys; print(int(datetime.datetime.fromisoformat(sys.argv[1].replace("Z","+00:00")).timestamp()))' \
                    "$completed_cutoff" \
                    2>/dev/null
            )"
            if [[ ! "$completed_epoch" =~ ^[0-9]+$ ]]; then
                echo "KLAVIYO_CATCHUP_FAIL invalid completed cutoff" >&2
                write_failure_latch 2 invalid_completed_cutoff || true
                exit 2
            fi
            # A completed cohort only ends the catch-up if its cutoff is recent;
            # otherwise the next cohort starts from a fresh cutoff and we go on.
            now_epoch="$("$date_bin" -u +%s)"
            completed_age_seconds=$((now_epoch - completed_epoch))
            if [ "$completed_age_seconds" -lt 0 ]; then
                completed_age_seconds=0
            fi
            if [ "$completed_age_seconds" -le "$max_completed_cutoff_age_seconds" ]; then
                echo "KLAVIYO_CATCHUP_COMPLETE iterations=$iteration cutoff=$completed_cutoff"
                exit 0
            fi
            echo "KLAVIYO_CATCHUP_COHORT_ADVANCED cutoff=$completed_cutoff age_seconds=$completed_age_seconds continuing_to_fresh_cutoff"
        fi

        if [ "$sleep_seconds" -gt 0 ]; then
            "$sleep_bin" "$sleep_seconds"
        fi
    done
done
