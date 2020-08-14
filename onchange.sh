#!/bin/sh

set -e
set -u
set -o pipefail

UUID=""
NAME=""
EVENT=""
PROPERTY=""
VALUE=""

while getopts 'i:n:p:e:k:v:' OPTION; do
  case "$OPTION" in

    i)
      UUID=$OPTARG
      echo "UUID is $OPTARG"
      ;;

    n)
      NAME=$OPTARG
      echo "Name is $OPTARG"
      ;;

    e)
      EVENT=$OPTARG
      echo "event is $OPTARG"
      ;;

    k)
      PROPERTY=$OPTARG
      echo "property key is $OPTARG"
      ;;

    v)
      VALUE=$OPTARG
      echo "property value is $OPTARG"
      ;;

    ?)
      echo "script usage: $(basename $0) [-i UUID] [-n name] [-e event] [-k property key] [-v property value]" >&2
      exit 1
      ;;

  esac
done
shift "$(($OPTIND -1))"


if [ -n "$UUID" ]; then
  echo "UUID: $UUID"
fi

if [ -n "$NAME" ]; then
  echo "Name: $NAME"
fi

if [ -n "$EVENT" ]; then
  echo "Event: $EVENT"
fi

if [ -n "$PROPERTY" ]; then
  echo "Property: $PROPERTY"
fi

if [ -n "$VALUE" ]; then
  echo "Value: $VALUE"
fi
