#!/bin/bash

set -e

# if given a command, run that
if [[ -n "$1" ]]; then
    exec "$@"
fi

while true;do /usr/local/bin/foo;sleep 30;done
