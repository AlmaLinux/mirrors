#!/bin/sh

# https://github.com/eficode/wait-for/blob/master/wait-for

TIMEOUT=15
QUIET=0

echoerr() {
    if [ "$QUIET" -ne 1 ]; then printf "%s\n" "$*" 1>&2; fi
}

usage() {
  exitcode="$1"
  cat << USAGE >&2
Usage:
  $cmdname host:port [-t timeout] [-- command args]
  -q | --quiet                        Do not output any status messages
  -t TIMEOUT | --timeout=timeout      Timeout in seconds, zero for no timeout
  -- COMMAND ARGS                     Execute command with args after the test finishes
USAGE
  exit "$exitcode"
}

wait_for() {
    host=$1
    port=$2

    echo "TRYING TO RESOLVE: ${host}:${port}"
    for i in `seq $TIMEOUT` ; do
        nc -z "$host" "$port"

        result=$?
        if [ $result -eq 0 ] ; then
            return 0
        fi
        sleep 1
    done
    echo "Operation timed out" >&2
    return 1
}

wait_for_all() {
    echo "ADDRESSES TO RESOLVE: ${ADDR}"
    for address in ${ADDR}; do
        HOST=$(echo "${address}" | cut -d : -f 1)
        PORT=$(echo "${address}" | cut -d : -f 2)
        wait_for "${HOST}" "${PORT}"
        wait_rc=$?
        if [ $wait_rc -ne 0 ]; then
            echo "Wait for ${address} failed"
            exit $wait_rc
        fi
    done
    exec "$@"
}

while [ $# -gt 0 ]
do
    case "$1" in
        *:* )
            ADDR="${ADDR} $(printf "%s\n" "$1")"
            shift 1
            ;;
        -q | --quiet)
            QUIET=1
            shift 1
            ;;
        -t)
            TIMEOUT="$2"
            if [ "$TIMEOUT" = "" ]; then break; fi
            shift 2
            ;;
        --timeout=*)
            TIMEOUT="${1#*=}"
            shift 1
            ;;
        --)
            shift
            break
            ;;
        --help)
            usage 0
            ;;
        *)
            echoerr "Unknown argument: $1"
            usage 1
            ;;
    esac
done

if [ "$ADDR" = "" ]; then
    echoerr "Error: you need to provide a host and port to test."
    usage 2
fi

wait_for_all "$@"
