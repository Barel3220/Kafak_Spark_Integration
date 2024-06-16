import matplotlib.pyplot as plt
import pandas as pd
import time
from pyspark.sql import SparkSession

# Initialize Spark session with specific Hadoop configuration
spark = SparkSession.builder \
    .appName("RealtimeVisualization") \
    .getOrCreate()

# Set up interactive mode
plt.ion()
fig, ax = plt.subplots(figsize=(10, 6))


# Function to plot real-time data
def plot_real_time_data():
    try:
        # Read data from the Parquet files
        df = spark.read.parquet("/tmp/spark-streaming-output")

        # Convert to Pandas DataFrame
        pd_df = df.toPandas()

        # Ensure start time is in datetime format
        pd_df['start'] = pd.to_datetime(pd_df['start'])

        # Clear the previous plot
        ax.clear()

        # Plot a subset of the data as scatter plot
        subset_df = pd_df.sample(n=min(len(pd_df), 1000))  # Plot at most 1000 points
        for key, grp in subset_df.groupby(['id']):
            ax.scatter(grp['start'], grp['count'], label=key, alpha=0.6)

        # Highlight the latest data points
        latest_time = pd_df['start'].max()
        latest_entries = pd_df[pd_df['start'] == latest_time]
        ax.scatter(latest_entries['start'], latest_entries['count'], color='red', label='Latest Entry',
                   edgecolor='black')

        ax.set_xlabel('Time')
        ax.set_ylabel('Campaign Count')
        ax.set_title('Real-time Campaign Activity')
        ax.legend()
        plt.xticks(rotation=45)
        plt.tight_layout()

        # Draw the updated plot
        fig.canvas.draw()
        fig.canvas.flush_events()
    except Exception as e:
        print(f"Error while plotting: {e}")


# Continuously update the plot every 10 seconds
while True:
    plot_real_time_data()
    time.sleep(10)
