#!/usr/bin/env bash

PROTO_DIR=$(dirname "$0")
PROTO_SRC_DIR=src/main/java/

#for i in CompactionClient CompactionService; do protoc $PROTO_DIR/asset-properties.proto --java_out=$PROTO_DIR/../$i/$PROTO_SRC_DIR; done
protoc $PROTO_DIR/asset-properties.proto --java_out=$PROTO_DIR/../Utils/$PROTO_SRC_DIR
#protoc asset-properties.proto --java_out=$PROTO_DIR/
