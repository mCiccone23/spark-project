from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag
from pyspark.sql.window import Window
import matplotlib.pyplot as plt
import pandas as pd
from google.cloud import storage

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Time-to-Failure Analysis") \
    .getOrCreate()

# File paths
machine_events_path = "gs://clusterdata_2019_a/machine_events-*.parquet.gz"
plot_output_path = "time_to_failure_histogram.png"
bucket_name = "spark-project-ciccone"

# Initialize Google Cloud Storage client
storage_client = storage.Client()

try:
    print("Loading machine events dataset...")
    # Load machine events dataset
    machine_events_df = spark.read.parquet(machine_events_path)
    print("Machine events dataset loaded successfully. Sample data:")
    machine_events_df.show(5)

    print("Filtering failure events...")
    # Filter failure events (type = 2 for failures)
    failure_events_df = machine_events_df.filter(col("type") == 2)
    print("Failure events filtered successfully. Sample data:")
    failure_events_df.show(5)

    print("Performing time-to-failure analysis...")
    # Time-to-failure analysis
    window_spec = Window.partitionBy("machine_id").orderBy(col("time"))
    failure_events_df = failure_events_df.withColumn("prev_time", lag("time").over(window_spec))
    failure_events_df = failure_events_df.withColumn("time_to_failure", \
                                                     (col("time") - col("prev_time")) / 1e9)  # Convert to seconds
    time_to_failure_df = failure_events_df.select("machine_id", "time", "time_to_failure").filter(col("time_to_failure").isNotNull())
    print("Time-to-failure intervals calculated. Sample data:")
    time_to_failure_df.show(5)

    print("Converting Spark DataFrame to Pandas DataFrame for visualization...")
    # Convert to Pandas DataFrame for visualization
    time_to_failure_pd = time_to_failure_df.toPandas()

    print("Creating time-to-failure histogram...")
    # Plot histogram of time-to-failure intervals
    plt.figure(figsize=(12, 6))
    plt.hist(time_to_failure_pd["time_to_failure"], bins=50, color="orange", edgecolor="black")
    plt.xlabel("Time to Failure (seconds)")
    plt.ylabel("Frequency")
    plt.title("Histogram of Time to Failure Intervals")
    plt.tight_layout()

    # Save plot locally
    local_time_to_failure_plot_path = "/tmp/time_to_failure_histogram.png"
    plt.savefig(local_time_to_failure_plot_path)
    print(f"Time-to-failure histogram saved locally at: {local_time_to_failure_plot_path}")

    print("Uploading time-to-failure histogram to GCS...")
    # Upload time-to-failure histogram to GCS
    bucket = storage_client.bucket(bucket_name)
    time_to_failure_blob = bucket.blob(f"time_to_failure_histogram.png")
    with open(local_time_to_failure_plot_path, "rb") as plot_file:
        time_to_failure_blob.upload_from_file(plot_file, content_type="image/png")
    print(f"Time-to-failure histogram successfully uploaded to GCS bucket: gs://{bucket_name}/time_to_failure_histogram.png")

except Exception as e:
    print("Error during analysis:", e)

# Stop SparkSession
spark.stop()
print("Spark session stopped.")