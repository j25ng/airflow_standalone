#!/bin/bash

YYYYMMDD=$1 #$no : shell에서 받아오는 매개변수

echo "check"
DONE_PATH=~/data/done/${YYYYMMDD}
DONE_PATH_FILE="${DONE_PATH}/_DONE"

if [ -e "$DONE_PATH_FILE" ]; then
        figlet "Let's move on"
        exit 0
else
        echo "I'll be back => $DONE_PATH_FILE"
        exit 1
fi
