Mongodb Development Cluster

This is a simple script to start up a sharded and/or replicated mongodb server setup.

Examples:

Simple replica set (3 members):

    node cluster.js --replicated


Simple sharded/replicated setup (3 shards, each shard is a 3 member replica set)

    node cluster.js --replicated --sharded --shardCount 3


