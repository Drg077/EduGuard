#!/usr/bin/env bash
set -e

echo "=== Initializing EduGuard Hadoop HDFS Node ==="

mkdir -p /opt/hadoop/data/name
mkdir -p /opt/hadoop/data/data
chown -R hadoop:users /opt/hadoop/data || true

export HDFS_NAMENODE_USER=root
export HDFS_DATANODE_USER=root
export HDFS_SECONDARYNAMENODE_USER=root
export YARN_RESOURCEMANAGER_USER=root
export YARN_NODEMANAGER_USER=root

if [ ! -d "/opt/hadoop/data/name/current" ]; then
    echo "Formatting Hadoop NameNode..."
    hdfs namenode -format -force -nonInteractive
fi

echo "Starting NameNode daemon..."
hdfs --daemon start namenode

echo "Starting DataNode daemon..."
hdfs --daemon start datanode

echo "Waiting for NameNode RPC to respond..."
until hdfs dfsadmin -report >/dev/null 2>&1; do
    echo "Waiting for HDFS cluster readiness..."
    sleep 2
done

echo "Ensuring SafeMode is off..."
hdfs dfsadmin -safemode leave || true

echo "Creating EduGuard HDFS directory structure..."
hdfs dfs -mkdir -p /eduguard/incoming
hdfs dfs -mkdir -p /eduguard/processed
hdfs dfs -mkdir -p /eduguard/predictions
hdfs dfs -mkdir -p /eduguard/model-data
hdfs dfs -chmod -R 777 /eduguard

echo "Starting HttpFS gateway on port 14000..."
hdfs --daemon start httpfs || true

echo "EduGuard Hadoop HDFS is ready and serving."
tail -f /opt/hadoop/logs/* 2>/dev/null || sleep infinity
