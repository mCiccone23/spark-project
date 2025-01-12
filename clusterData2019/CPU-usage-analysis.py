from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, col
import matplotlib.pyplot as plt
import pandas as pd
import io
from google.cloud import storage

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Average CPU Usage Analysis") \
    .getOrCreate()

# File path for events and usage
events_path = "gs://clusterdata_2019_a/instance_events-*.json.gz" 
usage_path = "gs://clusterdata_2019_a/instance_usage-*.json.gz"
output_path_csv = "gs://spark-project-ciccone/average_cpu_usage_results/"
plot_path = "average_cpu_usage_plot.png"  # File name for the plot in GCS

# Initialize Google Cloud Storage client
storage_client = storage.Client()
bucket_name = "spark-project-ciccone"

try:
    # Read the events and usage data
    events_df = spark.read.json(events_path)
    usage_df = spark.read.json(usage_path)

    # Filter events for Finished and Failed jobs
    filtered_events = events_df.filter(col("type").isin("FINISH", "FAIL"))

    # Join usage data with events to get CPU usage for each job
    joined_df = usage_df.join(filtered_events, ["collection_id", "instance_index"], "inner")

    # Calculate average CPU usage for Finished and Failed jobs
    avg_cpu_df = joined_df.groupBy("type", "collection_id").agg(
        mean("average_usage.cpus").alias("avg_cpu")
    )

    # Convert to Pandas DataFrame for plotting
    pd_df = avg_cpu_df.toPandas()

    # Save results to CSV in GCS
    avg_cpu_df.write.csv(output_path_csv, header=True, mode="overwrite")
    print(f"Results saved to GCS bucket: {output_path_csv}")

    try:
        # Separate data for Finished and Failed jobs
        finished_jobs = pd_df[pd_df["type"] == "FINISH"]
        failed_jobs = pd_df[pd_df["type"] == "FAIL"]

        # Plot the results
        plt.figure(figsize=(10, 6))
        plt.scatter(finished_jobs.index, finished_jobs["avg_cpu"], label="Finished Jobs", color="green", alpha=0.6)
        plt.scatter(failed_jobs.index, failed_jobs["avg_cpu"], label="Failed Jobs", color="red", alpha=0.6)
        plt.xlabel("Number of Jobs")
        plt.ylabel("Resources (GCU)")
        plt.title("Average CPU Usage Comparison")
        plt.legend()
        plt.tight_layout()

        # Save the plot locally for verification
        local_plot_path = "/tmp/average_cpu_usage_plot.png"
        plt.savefig(local_plot_path)
        print(f"Plot saved locally at: {local_plot_path}")

        # Upload to GCS
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(f"average_cpu_usage_plot.png")
        with open(local_plot_path, "rb") as plot_file:
            blob.upload_from_file(plot_file, content_type="image/png")
        print(f"Plot successfully uploaded to GCS bucket: gs://{bucket_name}/average_cpu_usage_plot.png")

    except Exception as upload_error:
        print("Error during plot generation or upload:", upload_error)

except Exception as e:
    print("Error during the elaboration:", e)

# Close Spark session
spark.stop()
