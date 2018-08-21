#!/bin/bash

if [ $# != 1 ]; then 
  echo "usage: $0 [--deploy || --undeploy]"
  exit 1;
fi

OKAPI="192.168.56.103:9130"
MD="target/ModuleDescriptor.json"
DD="target/DeploymentDescriptor.json"

echo "using Okapi @$OKAPI..."

SRVCID=`sed -nr 's/.*srvcId.*"(.*)".*/\1/p' $DD`
INSTID=`sed -nr 's/.*instId.*"(.*)".*/\1/p' $DD`

if [ "$INSTID" = "" ]; then
  INSTID=$SRVCID
fi

echo "using srvcId: $SRVCID"
echo "using instId: $INSTID"

if [ ${MD:+1} ] && [ ${DD:+1} ]; then
  if [ $1 == "--deploy" ]; then  
    URL=`cat target/DeploymentDescriptor.json | jq -r '.url'`
    curl -s $URL
    if [ $? -ne 0 ]; then
      echo "no service available at $URL. exiting."
      exit 1
    fi

    echo "posting ModuleDescriptor..."
    curl -w '\n' -X POST -D - -H "Content-type: application/json" -d @$MD http://$OKAPI/_/proxy/modules
    sleep .5

    echo "posting DeploymentDescriptor..."
    curl -w '\n' -D - -s -X POST -H "Content-type: application/json" -d @$DD http://$OKAPI/_/discovery/modules
    sleep .5

    echo "enabling for diku tenant..."
    curl -w '\n' -X POST -D - -H "Content-type: application/json" -d "{ \"id\" : \"$SRVCID\" }" http://$OKAPI/_/proxy/tenants/diku/modules
  fi

  if [ $1 == "--undeploy" ]; then
    echo "deleting module for tenant..."
    curl -w '\n' -X DELETE  -D -    http://$OKAPI/_/proxy/tenants/diku/modules/$SRVCID
    sleep .5

    echo "deleting DeploymentDescriptor..."
    curl -w '\n' -X DELETE  -D -    http://$OKAPI/_/discovery/modules/$SRVCID/$INSTID
    sleep .5

    echo "deleting ModuleDescriptor..."
    curl -w '\n' -X DELETE  -D -    http://$OKAPI/_/proxy/modules/$SRVCID
  fi
fi
