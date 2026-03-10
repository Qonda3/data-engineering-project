#!/bin/bash
docker compose down;
docker container list -a | grep wtc | tr -s ' ' | cut -d' ' -f 1 | xargs docker container stop;
docker container list -a | grep wtc | tr -s ' ' | cut -d' ' -f 1 | xargs docker container rm;
docker image ls | tr -s ' ' | grep wtc | cut -d' ' -f 3 | xargs docker image rm;
rm -rf ./volumes/data;
sleep 5;
docker compose up -d;

if [ "$?" != "0" ]; then
  echo "ERROR: Failed to start all required containers";
  exit 1
fi

#########################
# Add your scripting here
#########################
./register-connector.sh
