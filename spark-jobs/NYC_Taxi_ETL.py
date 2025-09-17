#!/usr/bin/env python

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

def main():
    """
    ETL job to process NYC Yellow Taxi data, clean it, enrich it with new features
    and location information, and save the final result to PostgreSQL.
    """
    # 1. Initialize Spark Session
    spark = SparkSession.builder \
        .appName('NYC_Taxi_ETL_Job') \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar") \
        .config("spark.driver.extraClassPath", "/opt/spark/jars/postgresql-42.6.0.jar") \
        .getOrCreate()

    # 2. Define Schemas
    # Schema for the main taxi trip data
    taxi_schema = StructType([
        StructField("VendorID", IntegerType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", LongType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("RatecodeID", LongType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("PULocationID", IntegerType(), True),
        StructField("DOLocationID", IntegerType(), True),
        StructField("payment_type", LongType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
        StructField("Airport_fee", DoubleType(), True)
    ])

    # 3. Data Ingestion from HDFS
    # Load the raw taxi dataset from an HDFS Parquet file
    df_raw = spark.read.schema(taxi_schema).parquet('hdfs://namenode:8020/data/yellow_tripdata_2024-01.parquet')

    # Load the location lookup table from HDFS
    location_lkp = spark.read.csv('hdfs://namenode:8020/data/location_lookup.csv', header=True, inferSchema=True)

    # 4. Data Cleaning and Transformation
    # Standardize column names by stripping whitespace
    df_renamed = df_raw
    for c in df_renamed.columns:
        df_renamed = df_renamed.withColumnRenamed(c, c.strip())

    # Drop columns that are not required for the final dataset
    df_dropped = df_renamed.drop(
        'RatecodeID',
        'store_and_fwd_flag',
        'improvement_surcharge',
        'congestion_surcharge',
        'Airport_fee'
    )

    # Filter rows based on logical business rules to ensure data quality
    df_cleaned = df_dropped.filter(
        (F.col('tpep_pickup_datetime').isNotNull()) &
        (F.col('tpep_dropoff_datetime').isNotNull()) &
        (F.col('trip_distance').between(0.1, 300)) &
        (F.col('total_amount') >= 0)
    )

    # Cache the cleaned DataFrame to speed up subsequent actions
    df_cleaned.persist()

    # 5. Feature Engineering
    # Create new features from existing columns to enrich the dataset
    df_enriched = (df_cleaned
        # Calculate the total trip duration in seconds
        .withColumn("trip_duration_sec",
                    F.unix_timestamp(F.col("tpep_dropoff_datetime")) - F.unix_timestamp(F.col("tpep_pickup_datetime")))
        # Calculate the average speed in miles per hour
        .withColumn("average_speed_mph",
                    F.round(F.col('trip_distance') / (F.col('trip_duration_sec') / 3600), 2))
        # Categorize trips based on their distance
        .withColumn('trip_category',
                    F.when(F.col('trip_distance') <= 2, 'short')
                     .when((F.col('trip_distance') > 2) & (F.col('trip_distance') <= 10), 'medium')
                     .otherwise('long'))
        # Extract time-based features from the pickup datetime
        .withColumn('pickup_hour', F.hour('tpep_pickup_datetime'))
        .withColumn('day_of_week', F.dayofweek('tpep_pickup_datetime'))
        .withColumn('month', F.month('tpep_pickup_datetime'))
        # Determine if the trip occurred on a weekend or weekday
        .withColumn('is_weekend',
                    F.when(F.col('day_of_week').isin([1, 7]), 'Weekend')
                     .otherwise('Weekday'))
    )

    # 6. Data Enrichment via Broadcast Join
    # F.broadcast() sends the small lookup DataFrame to all worker nodes for an efficient join.
    df_joined = df_enriched.join(
        F.broadcast(location_lkp),
        df_enriched['PULocationID'] == location_lkp['LocationID'],
        how='left'
    ).select(
        df_enriched['*'],
        location_lkp['Borough'],
        location_lkp['Zone']
    )

    # 7. Save Final Dataset to PostgreSQL
    (df_joined.write.format("jdbc")
     .option("url", "jdbc:postgresql://postgres:5432/spark")
     .option("driver", "org.postgresql.Driver")
     .option("dbtable", "nyc_taxi_trips")
     .option("user", "spark")
     .option("password", "spark")
     .mode("overwrite")
     .save())

    # Unpersist the cached DataFrame to free up memory
    df_cleaned.unpersist()

    # Stop the Spark session
    spark.stop()


if __name__ == '__main__':
    main()
