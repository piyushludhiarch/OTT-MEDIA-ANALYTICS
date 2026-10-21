#!/bin/bash

MYSQL_HOST="localhost"
MYSQL_PORT="3306"
MYSQL_DB="ott_media"
MYSQL_USER="sqoop_user"

HDFS_BASE="/ott_media/raw"

echo "Enter MySQL password:"
read -s MYSQL_PASSWORD
echo

echo "Starting Sqoop imports..."

sqoop import \
--connect "jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/${MYSQL_DB}" \
--username "$MYSQL_USER" \
--password "$MYSQL_PASSWORD" \
--table plans \
--target-dir "${HDFS_BASE}/plans" \
--delete-target-dir \
--num-mappers 1

if [ $? -ne 0 ]; then
    echo "ERROR: plans import failed"
    exit 1
fi

echo "plans import completed."

sqoop import \
--connect "jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/${MYSQL_DB}" \
--username "$MYSQL_USER" \
--password "$MYSQL_PASSWORD" \
--table subscribers \
--target-dir "${HDFS_BASE}/subscribers" \
--delete-target-dir \
--num-mappers 1

if [ $? -ne 0 ]; then
    echo "ERROR: subscribers import failed"
    exit 1
fi

echo "subscribers import completed."

sqoop import \
--connect "jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/${MYSQL_DB}" \
--username "$MYSQL_USER" \
--password "$MYSQL_PASSWORD" \
--table content \
--target-dir "${HDFS_BASE}/content" \
--delete-target-dir \
--num-mappers 1

if [ $? -ne 0 ]; then
    echo "ERROR: content import failed"
    exit 1
fi

echo "content import completed."
echo "All Sqoop imports completed successfully."