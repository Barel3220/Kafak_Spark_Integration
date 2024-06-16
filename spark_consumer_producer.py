import datetime
import json
import os
import sys

from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, from_json
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import tldextract
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline

# Define the base path for saving output and checkpoints
base_path = os.path.dirname(os.path.abspath(sys.argv[0]))

parquet_output_path = os.path.join(base_path, "tmp/output")
parquet_checkpoint_path = os.path.join(base_path, "tmp/checkpoint")

# Create a Spark session
spark = SparkSession.builder \
    .appName("URL Clustering") \
    .getOrCreate()


# Define a function to extract the main domain from the URL
def extract_main_domain(url):
    """Extracts the main domain from a URL using tldextract.

    Args:
        url (str): The URL to extract the main domain from.

    Returns:
        str: The main domain extracted from the URL.
    """
    ext = tldextract.extract(url)
    return ext.domain


# Register the function as a UDF
extract_main_domain_udf = udf(extract_main_domain, StringType())

# Define the schema for the incoming data
schema = StructType([
    StructField("creation_time", TimestampType(), True),
    StructField("id", StringType(), True),
    StructField("url", StringType(), True),
    StructField("title", StringType(), True),
    StructField("keywords", StringType(), True),
    StructField("description", StringType(), True)
])

# Read data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "topic_url") \
    .load()

# Parse the JSON data and extract fields
df = df.selectExpr("CAST(value AS STRING) as json") \
       .select(from_json(col("json"), schema).alias("data")) \
       .select("data.*")

# Extract the main domain from the URL
df = df.withColumn("main_domain", extract_main_domain_udf(df.url))

# Define the stages for the pipeline
tokenizer = Tokenizer(inputCol="main_domain", outputCol="tokens")
hashed = HashingTF(inputCol="tokens", outputCol="rawFeatures", numFeatures=50)
idf = IDF(inputCol="rawFeatures", outputCol="features")
scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")

# Perform K-Means clustering with a higher K
kmeans = KMeans().setK(50).setSeed(1).setFeaturesCol("scaledFeatures").setMaxIter(100)
pipeline = Pipeline(stages=[tokenizer, hashed, idf, scaler, kmeans])


# Define a JSON serializer for non-serializable objects
def json_serializer(obj):
    """JSON serializer for objects not serializable by default.

    Args:
        obj: The object to serialize.

    Returns:
        str: JSON serialized string.

    Raises:
        TypeError: If the object type is not serializable.
    """
    if isinstance(obj, (datetime.date, datetime.datetime)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


# Define a function to produce messages to Kafka
def produce_to_kafka(row):
    """Produces messages to Kafka topics based on the cluster assignment.

    Args:
        row (Row): The row of data to send to Kafka.
    """
    producer_conf = {'bootstrap_servers': 'localhost:9092',
                     'value_serializer': lambda v: json.dumps(v, default=json_serializer).encode('utf-8')}
    producer = KafkaProducer(**producer_conf)
    topic = f"cluster_{row.cluster}"
    value = {"id": row.id, "creation_time": row.creation_time,
             "main_domain": row.main_domain, "description": row.description}
    producer.send(topic, value)
    producer.flush()


# Define a function to process each batch of data
def process_batch(batch_df, batch_id):
    """Processes each batch of data, performs clustering, and writes results to Kafka and Parquet.

    Args:
        batch_df (DataFrame): The batch DataFrame.
        batch_id (int): The batch ID.
    """
    if batch_df.count() > 0:
        model = pipeline.fit(batch_df)
        result_df = model.transform(batch_df)
        result_df = result_df.select(col("url"), col("main_domain"), col("prediction").alias("cluster"), col("id"),
                                     col("creation_time"), col("description"))

        result_df.write.mode("append").parquet(parquet_output_path)

        # Send each row to the corresponding Kafka topic
        result_df.foreach(produce_to_kafka)
    else:
        print(f"Batch {batch_id} is empty. Skipping.")


# Start the streaming query
query = df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .option("checkpointLocation", parquet_checkpoint_path) \
    .start()

query.awaitTermination()
