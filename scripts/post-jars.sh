#!/bin/bash

# Grab jars necessary for this project and post them to an S3 bucket,
# derived from instructions here:
# http://eric.lubow.org/2011/hadoop/pig-queries-parsing-json-on-amazons-elastic-map-reduce-using-s3-data/

set -e

if [ 1 -ne $# ]; then
    echo "usage $0 s3://bucket/folder"
    exit 1;
fi

BUCKET=$1


# Download and extract the project
REMOTETGZ=https://github.com/kevinweil/elephant-bird/tarball/eb1.2.1_with_jsonloader  
wget --no-check-certificate $REMOTETGZ -O /tmp/eb.1.2.1_with_jsonloader
cd /tmp
tar -xzvf eb.1.2.1_with_jsonloader

TOPDIR='/tmp/kevinweil-elephant-bird*'


# Build something or other
cd $TOPDIR
ant nonothing
cd build/classes
jar -cf ../elephant-bird-1.2.1-SNAPSHOT.jar com

# Push all of the jars to s3
s3cmd put $TOPDIR/lib/google-collect-1.0.jar $TOPDIR/lib/json-simple-1.1.jar $TOPDIR/build/*.jar $BUCKET/

