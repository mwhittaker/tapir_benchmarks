#! /usr/bin/env bash

set -euo pipefail

usage() {
    echo "run_client.sh" \
            "[-f <f>]" \
            "[-N <num_shards>]" \
            "[-t <num_clients>]" \
            "[-o <output_dir>]" \
            "<client_id>"
}

main() {
	f=1
    num_shards=5
    num_clients=10
    output_dir="/tmp"
    while getopts ":hf:N:t:o:" opt; do
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
            t )
                num_clients="$OPTARG"
                ;;
            o )
                output_dir="$OPTARG"
                ;;
            \? )
                echo "Unknown argument."
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

    local -r client_id="$1"
    local -r session_name=clients

    tmux kill-session -t "$session_name" || true
    tmux new-session -s "$session_name" -d
    for cpu in $(seq 0 "$((num_clients - 1))"); do
        tmux new-window -t "$session_name:" -n "client$cpu"
        tmux send-keys -t "$session_name:client$cpu" \
            "./store/benchmark/benchClient " \
                "-c $HOME/config/f${f}.shard " \
                "-f $HOME/config/keys.txt " \
                "-N ${num_shards} " \
                "-d 60 " \
                "-l 5 " \
                "-w 50 " \
                "-k 100000 " \
                "-r 0 " \
                "-m txn-l " \
                "-z 0 " \
            "2>&1 | tee ${output_dir}/${client_id}.${cpu}.csv" \
            C-m
    done
}

main "$@"
