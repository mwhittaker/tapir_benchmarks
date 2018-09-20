#! /usr/bin/env bash

set -euo pipefail

usage() {
    echo 'run_server.sh [-f <f>] [-N <num_shards>] <replica_index>'
}

main() {
	f=1
    num_shards=5
    while getopts ":hf:N:" opt; do
        case ${opt} in
            h )
                usage "$0"
                return 0
                ;;
            f )
                f="$OPTARG"
                ;;
            N )
                num_shards="$OPTARG"
                ;;
            \? )
                usage "$0"
                return 1
                ;;
            : )
                echo "$OPTARG reqiures an argument."
                usage "$0"
                return 1
                ;;
        esac
    done
    shift $((OPTIND -1))

    if [[ "$#" -ne 1 ]]; then
        usage
        exit 1
    fi

    local -r replica_index="$1"
    local -r session_name=servers

    tmux kill-session -t "$session_name" || true
    tmux new-session -s "$session_name" -d
    for shard in $(seq 0 "$((num_shards - 1))"); do
        tmux new-window -t "$session_name:" -n "shard$shard"
        tmux send-keys -t "$session_name:shard$shard" \
            "taskset -c $shard ./store/tapirstore/server " \
                "-c $HOME/config/f${f}.shard${shard}.config " \
                "-i $replica_index " \
                "-n $shard " \
                "-N $num_shards " \
                "-f $HOME/config/keys.txt " \
                "-k $((100 * 1000)) " \
                "-m txn-l " \
            C-m
    done
}

main "$@"
