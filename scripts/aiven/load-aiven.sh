#!/bin/bash

BULK_DATA=~/bulk_data
AVN_SERVICE_NAME=${NAME:-clickhouse-demo}
AVN_SERVICE_CLOUD=${CLOUD:-google-us-west1}
AVN_SERVICE_PLAN=${PLAN:-startup-beta-16}
BATCH=${BATCH:-50000}


# Now that data has been create, lets bring up our clickhouse service
avn service create \
  --service-type clickhouse \
  --plan $AVN_SERVICE_PLAN \
  --cloud $AVN_SERVICE_CLOUD \
  --no-project-vpc \
  $AVN_SERVICE_NAME

while [ "RUNNING" != $(avn service get $AVN_SERVICE_NAME --format '{state}') ]
do
  echo "Service $AVN_SERVICE_NAME is being built.."
  sleep 20
done

# Now lets load the data
cd load

eval $(avn service user-list --format \
  'DATABASE_PASSWORD={password} DATABASE_USER={username}' \
   ${AVN_SERVICE_NAME})

SERVICE_URI=$(avn service get $AVN_SERVICE_NAME --format '{service_uri}')

BATCH_SIZE=$BATCH \
  BULK_DATA_DIR=$BULK_DATA \
  DATABASE_HOST=$(echo $SERVICE_URI|cut -d: -f1) \
  DATABASE_PORT=$(echo $SERVICE_URI|cut -d: -f2) \
  DATABASE_USER=$DATABASE_USER \
  DATABASE_PASSWORD=$DATABASE_PASSWORD ./load_clickhouse.sh

