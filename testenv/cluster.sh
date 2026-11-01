#!/bin/bash
# Controls the SeaweedFS test cluster defined in compose.yaml. Run without arguments for usage.
# Needs curl and jq on the host.
set -euo pipefail
cd "$(dirname "$0")"

MASTERS=(master1 master2 master3)
VOLUMES=(dc1-vol1 dc1-vol2 dc2-vol1 dc2-vol2)
declare -A ADDR=(
    [master1]=172.29.0.11:9333 [master2]=172.29.0.12:9333 [master3]=172.29.0.13:9333
    [dc1-vol1]=172.29.0.21:8080 [dc1-vol2]=172.29.0.22:8080
    [dc2-vol1]=172.29.0.31:8080 [dc2-vol2]=172.29.0.32:8080
)

usage() {
    cat <<'EOF'
Usage: testenv/cluster.sh <command> [args]

  up                 start the cluster and wait until all masters follow one leader and the
                     leader knows all volume servers; a running cluster is kept as it is
  down               stop the cluster and throw away all data
  show               print each master's view of the leader, and the volumes of each volume
                     server according to the leader
  logs [server...]   follow the logs
  stop <server>      stop the server cleanly (docker compose stop)
  kill <server>      kill the server's container (SIGKILL, like a crash)
  start <server>     start a stopped or killed server again

Masters: master1 172.29.0.11:9333, master2 172.29.0.12:9333, master3 172.29.0.13:9333.
Volume servers: dc1-vol1 172.29.0.21:8080, dc1-vol2 172.29.0.22:8080 (data center dc1),
dc2-vol1 172.29.0.31:8080, dc2-vol2 172.29.0.32:8080 (data center dc2). New volumes get
replication 100: one copy in each data center.
EOF
}

dc() {
    docker compose "$@"
}

check_server() {
    if [ -z "${1:-}" ] || [ -z "${ADDR[$1]:-}" ]; then
        echo "Unknown server: '${1:-}'. Servers: ${MASTERS[*]} ${VOLUMES[*]}" >&2
        exit 2
    fi
}

# Prints the /cluster/status of a master, or nothing if it doesn't answer.
status() {
    curl -sf -m 1 "http://${ADDR[$1]}/cluster/status" 2>/dev/null || true
}

# Prints the leader that a master reports (without the gRPC port), or nothing.
leader_of() {
    status "$1" | jq -r '.Leader // empty | sub("\\.[0-9]+$"; "")' 2>/dev/null || true
}

# Prints the masters that say they are the leader.
leaders() {
    local m
    for m in "${MASTERS[@]}"; do
        if [ "$(status "$m" | jq -r '.IsLeader // false' 2>/dev/null)" = true ]; then
            echo "$m"
        fi
    done
}

# Prints "datacenter rack address volume-ids" for each volume server the master knows.
volume_servers() {
    curl -sf -m 2 "http://${ADDR[$1]}/vol/status" 2>/dev/null | jq -r '
        .Volumes.DataCenters // {} | to_entries[] | .key as $dc | .value | to_entries[] |
        .key as $rack | .value | to_entries[] |
        "\($dc) \($rack) \(.key) \([(.value // [])[].Id] | sort | map(tostring) | join(","))"' 2>/dev/null || true
}

server_at() {
    local s
    for s in "${!ADDR[@]}"; do
        if [ "${ADDR[$s]}" = "$1" ]; then
            echo "$s"
            return
        fi
    done
    echo "$1"
}

up() {
    dc up -d
    echo "Waiting for the cluster..."
    local leader known healthy m v
    for _ in $(seq 120); do
        leader=$(leaders)
        if [ "$(wc -w <<<"$leader")" -gt 1 ]; then
            echo "More than one master says it is the leader:" $leader >&2
        elif [ -n "$leader" ]; then
            healthy=1
            for m in "${MASTERS[@]}"; do
                if [ "$(leader_of "$m")" != "${ADDR[$leader]}" ]; then
                    healthy=0
                fi
            done
            known=$(volume_servers "$leader" | cut -d' ' -f3)
            for v in "${VOLUMES[@]}"; do
                if ! grep -qx "${ADDR[$v]}" <<<"$known"; then
                    healthy=0
                fi
            done
            if [ "$healthy" = 1 ]; then
                show
                return
            fi
        fi
        sleep 1
    done
    echo "The cluster did not become healthy in 2 minutes. See '$0 show' and '$0 logs'." >&2
    exit 1
}

show() {
    local m leader="" line dc rack addr ids
    for m in "${MASTERS[@]}"; do
        local st
        st=$(status "$m")
        if [ -z "$st" ]; then
            echo "== $m ${ADDR[$m]}  not answering"
        elif [ "$(jq -r '.IsLeader // false' <<<"$st")" = true ]; then
            echo "== $m ${ADDR[$m]}  leader"
            leader=$m
        else
            echo "== $m ${ADDR[$m]}  follower of $(server_at "$(leader_of "$m")")"
        fi
    done
    if [ -z "$leader" ]; then
        echo "No leader"
        return
    fi
    while read -r dc rack addr ids; do
        [ -n "$addr" ] || continue
        echo "   $dc/$rack $(server_at "$addr") $addr  volumes ${ids:-none}"
    done < <(volume_servers "$leader" | sort)
}

cmd=${1:-}
shift || true
case "$cmd" in
    up) up ;;
    down) dc down -v --remove-orphans ;;
    show) show ;;
    logs) dc logs -f "$@" ;;
    stop | kill | start)
        check_server "${1:-}"
        dc "$cmd" "$1"
        ;;
    *)
        usage
        [ -z "$cmd" ] || exit 2
        ;;
esac
