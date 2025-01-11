from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, max
import matplotlib.pyplot as plt
import pandas as pd
from google.cloud import storage

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("CPU and Memory Utilization Analysis") \
    .getOrCreate()

# File paths
instance_usage_path = "gs://clusterdata_2019_a/instance_usage-000000011068.parquet.gz"
output_csv_path = "gs://spark-project-ciccone/cpu_memory_analysis_output/"
cpu_plot_output_path = "cpu_utilization_plot.png"  # File name for the CPU plot in GCS
memory_plot_output_path = "memory_utilization_plot.png"  # File name for the memory plot in GCS
bucket_name = "spark-project-ciccone"

# Initialize Google Cloud Storage client
storage_client = storage.Client()

try:
    print("Loading instance usage dataset...")
    # Load instance usage dataset
    instance_usage_df = spark.read.parquet(instance_usage_path)
    print("Dataset loaded successfully. Sample data:")
    instance_usage_df.show(5)

    print("Calculating average and peak CPU and memory utilization...")
    # Calculate average and peak CPU and memory utilization
    utilization_df = instance_usage_df.groupBy("collection_id").agg(
        mean("average_usage.cpus").alias("avg_cpu"),
        max("average_usage.cpus").alias("peak_cpu"),
        mean("average_usage.memory").alias("avg_memory"),
        max("average_usage.memory").alias("peak_memory")
    )
    print("Utilization metrics calculated successfully. Sample data:")
    utilization_df.show(5)

    print("Converting Spark DataFrame to Pandas DataFrame for visualization...")
    # Convert to Pandas DataFrame for visualization
    pd_df = utilization_df.toPandas()

    print("Creating CPU utilization plot...")
    # Plot CPU utilization
    plt.figure(figsize=(12, 6))
    plt.scatter(pd_df.index, pd_df["avg_cpu"], label="Average CPU Usage", color="blue", alpha=0.6)
    plt.scatter(pd_df.index, pd_df["peak_cpu"], label="Peak CPU Usage", color="red", alpha=0.6)
    plt.xlabel("Job Index")
    plt.ylabel("CPU Usage (GCU)")
    plt.title("CPU Utilization: Average vs Peak")
    plt.legend()
    plt.tight_layout()

    # Save CPU plot locally
    local_cpu_plot_path = "/tmp/cpu_utilization_plot.png"
    plt.savefig(local_cpu_plot_path)
    print(f"CPU plot saved locally at: {local_cpu_plot_path}")

    print("Uploading CPU plot to GCS...")
    # Upload CPU plot to GCS
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(f"cpu_memory_analysis/{cpu_plot_output_path}")
    with open(local_cpu_plot_path, "rb") as plot_file:
        blob.upload_from_file(plot_file, content_type="image/png")
    print(f"CPU plot successfully uploaded to GCS bucket: gs://{bucket_name}/cpu_memory_analysis/{cpu_plot_output_path}")

    print("Creating Memory utilization plot...")
    # Plot Memory utilization
    plt.figure(figsize=(12, 6))
    plt.scatter(pd_df.index, pd_df["avg_memory"], label="Average Memory Usage", color="green", alpha=0.6)
    plt.scatter(pd_df.index, pd_df["peak_memory"], label="Peak Memory Usage", color="orange", alpha=0.6)
    plt.xlabel("Job Index")
    plt.ylabel("Memory Usage (GB)")
    plt.title("Memory Utilization: Average vs Peak")
    plt.legend()
    plt.tight_layout()

    # Save Memory plot locally
    local_memory_plot_path = "/tmp/memory_utilization_plot.png"
    plt.savefig(local_memory_plot_path)
    print(f"Memory plot saved locally at: {local_memory_plot_path}")

    print("Uploading Memory plot to GCS...")
    # Upload Memory plot to GCS
    blob = bucket.blob(f"cpu_memory_analysis/{memory_plot_output_path}")
    with open(local_memory_plot_path, "rb") as plot_file:
        blob.upload_from_file(plot_file, content_type="image/png")
    print(f"Memory plot successfully uploaded to GCS bucket: gs://{bucket_name}/cpu_memory_analysis/{memory_plot_output_path}")

    print("Saving utilization metrics to GCS as CSV...")
    # Save results to CSV in GCS
    utilization_df.write.csv(output_csv_path, header=True, mode="overwrite")
    print(f"Results saved to GCS bucket: {output_csv_path}")

except Exception as e:
    print("Error during analysis:", e)

# Stop SparkSession
spark.stop()
print("Spark session stopped.")
