#!/bin/bash

# 1) modifies okapi endpoint url in DeploymentDescriptor
# 2) removes all permissions from ModuleDescriptor

TMP=`mktemp`
cat target/DeploymentDescriptor.json | jq 'del (.descriptor) | del(.nodeId) | .+{"url": "http://192.168.56.100:8081"} | .+{"instId": .srvcId}' > "$TMP" && mv "$TMP" target/DeploymentDescriptor.json

TMP=`mktemp`
cat target/ModuleDescriptor.json | jq 'del(.launchDescriptor) | del(.. | .permissionsRequired?)' > "$TMP" && mv "$TMP" target/ModuleDescriptor.json
