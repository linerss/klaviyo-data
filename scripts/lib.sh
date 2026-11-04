#!/usr/bin/env bash
# Shared helpers for the Klaviyo runners. Source this file; do not execute it.

klaviyo_project_root() {
    cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd
}

# Prints configured event metric IDs, one per line, from KLAVIYO_EVENT_METRICS
# (comma-separated). Fails if any ID is malformed.
klaviyo_metric_ids() {
    local raw="${KLAVIYO_EVENT_METRICS:-}"
    local id
    [ -z "${raw//[[:space:]]/}" ] && return 0
    # Empty entries (leading, trailing or doubled commas) are configuration errors.
    if [[ "$raw" =~ (^|,)[[:space:]]*(,|$) ]]; then
        echo "empty metric ID in KLAVIYO_EVENT_METRICS" >&2
        return 2
    fi
    local IFS=','
    for id in $raw; do
        # Trim surrounding whitespace only; inner whitespace stays and is rejected.
        id="${id#"${id%%[![:space:]]*}"}"
        id="${id%"${id##*[![:space:]]}"}"
        if [[ ! "$id" =~ ^[A-Za-z0-9]{1,32}$ ]]; then
            echo "invalid metric ID in KLAVIYO_EVENT_METRICS" >&2
            return 2
        fi
        printf '%s\n' "$id"
    done
}

# Prints every configured job key: profiles, then event:<id> per metric.
klaviyo_job_keys() {
    local ids
    ids="$(klaviyo_metric_ids)" || return 2
    echo profiles
    local id
    for id in $ids; do
        echo "event:$id"
    done
}

# Prints the fair rotation used by the cycle and catch-up runners: profiles
# get one slot before every event stream, so a large profile backlog never
# starves events and events never delay profiles by more than one slot.
klaviyo_rotation() {
    local ids
    ids="$(klaviyo_metric_ids)" || return 2
    if [ -z "$ids" ]; then
        echo profiles
        return 0
    fi
    local id
    for id in $ids; do
        echo profiles
        echo "event:$id"
    done
}

# Returns 0 if $1 is a configured job key.
klaviyo_is_configured_job() {
    local candidate="$1"
    local keys
    keys="$(klaviyo_job_keys)" || return 2
    local key
    for key in $keys; do
        [ "$key" = "$candidate" ] && return 0
    done
    return 1
}

klaviyo_default_classpath() {
    local root
    root="$(klaviyo_project_root)"
    printf '%s' "$root/target/classes:$root/target/lib/*"
}
