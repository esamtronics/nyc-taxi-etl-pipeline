#!/bin/bash
set -e

cd ~/projects/project3

echo "Setting docker compose down..."
docker compose down -v

echo "Starting Hadoop/Spark cluster..."
docker compose up -d

# Function to check if container is healthy
check_container_health() {
    local container=$1
    local max_attempts=$2
    local attempt=1
    
    echo "Waiting for $container to be healthy..."
    while [ $attempt -le $max_attempts ]; do
        if docker inspect --format='{{.State.Health.Status}}' $container 2>/dev/null | grep -q "healthy"; then
            echo "$container is healthy!"
            return 0
        fi
        echo "Attempt $attempt/$max_attempts: $container not ready yet..."
        sleep 5
        ((attempt++))
    done
    
    echo "Error: $container failed to become healthy"
    return 1
}

echo "Waiting for HDFS to be ready..."
datanodes_available=0
while [ $datanodes_available -lt 3 ]; do
    echo "Waiting for datanodes to be available..."
    
    report=$(docker exec namenode hdfs dfsadmin -report 2>/dev/null || echo "")

    # FIX: Check for "Live datanodes" which is present in the report
    if [[ $report == *"Live datanodes"* ]]; then
        # FIX: Parse the number from the "Live datanodes (3):" line
        datanodes_available=$(echo "$report" | grep "Live datanodes" | awk '{print $3}' | sed 's/[^0-9]//g')
        echo "$datanodes_available datanode(s) now available."
    else
        echo "HDFS service not fully up yet..."
        datanodes_available=0
    fi
    sleep 5
done

echo "All datanodes are available. HDFS is ready."


echo "Creating HDFS directory..."
docker exec namenode hdfs dfs -mkdir -p /data/

echo "Copying data to HDFS..."
docker exec namenode hdfs dfs -put /data/location_lookup.csv /data/
docker exec namenode hdfs dfs -put /data/yellow_tripdata_2024-01.parquet /data/

docker exec namenode hdfs dfs -ls /data/

echo "Data loaded into HDFS successfully."

echo "Submitting Spark job..."
docker exec spark-master spark-submit \
    --jars /opt/spark/jars/postgresql-42.6.0.jar \
    --driver-class-path /opt/spark/jars/postgresql-42.6.0.jar \
    --conf spark.executor.extraClassPath=/opt/spark/jars/postgresql-42.6.0.jar \
    /spark-jobs/NYC_Taxi_ETL.py

echo "Spark job submitted successfully."

echo "You can access pgAdmin at http://localhost:5050"
echo "Username: admin@admin.com"
echo "Password: admin"