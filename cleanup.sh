#!/usr/bin/env bash
set -euo pipefail
shopt -s nullglob

DRY=0
FORCE=0
for arg in "$@"; do
    case "$arg" in
        -n) DRY=1 ;;
        -f) FORCE=1 ;;
        -h|--help)
            echo "usage: cleanup.sh [-n] [-f]"
            echo "  -n  dry run (list what would be removed)"
            echo "  -f  skip the running-process warning/confirmation"
            exit 0
            ;;
        *)
            echo "cleanup.sh: unknown flag '$arg' (try -h)" >&2
            exit 2
            ;;
    esac
done

if [ "$FORCE" -eq 0 ]; then
    if pgrep -x 'producer|consumer|bridge_daemon|stress_test' >/dev/null 2>&1; then
        echo "WARNING: shm-telemetry processes appear to be running:" >&2
        pgrep -ax 'producer|consumer|bridge_daemon|stress_test' >&2 || true
        echo "Removing their shared-memory segments and semaphores will break them." >&2
        if [ "$DRY" -eq 1 ]; then
            echo "(dry run: continuing)" >&2
        else
            read -r -p "Continue? [y/N] " ans
            case "$ans" in
                y|Y|yes|YES) ;;
                *) echo "aborted" >&2; exit 1 ;;
            esac
        fi
    fi
fi

TOTAL=0

remove_path() {
    local p="$1"
    if [ "$DRY" -eq 1 ]; then
        echo "would remove $p"
    else
        rm -f -- "$p"
        echo "removed $p"
    fi
    TOTAL=$((TOTAL + 1))
}

for p in /dev/shm/shm_ring_* /dev/shm/shm_metrics_* \
         /dev/shm/sem.sem_free_* /dev/shm/sem.sem_occ_* \
         /tmp/shm_bridge_*.sock; do
    remove_path "$p"
done

if [ "$TOTAL" -eq 0 ]; then
    echo "nothing to remove"
else
    if [ "$DRY" -eq 1 ]; then
        echo "total: $TOTAL path(s) would be removed"
    else
        echo "total: $TOTAL path(s) removed"
    fi
fi

exit 0
