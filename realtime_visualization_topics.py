import time
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import sys

base_path = os.path.dirname(os.path.abspath(sys.argv[0]))

parquet_output_path = os.path.join(base_path, "tmp/output")


def load_parquet_files(path):
    # Load all Parquet files in the directory
    return pd.read_parquet(path)


def plot_clusters(df_, ax_):
    ax_.clear()  # Clear the previous plot
    sns.scatterplot(data=df_, x='main_domain', y='cluster', hue='cluster', palette='tab10', legend='full', ax=ax)
    ax_.set_title('Real-Time URL Clustering')
    ax_.set_xlabel('Main Domain')
    ax_.set_ylabel('Cluster')


fig, ax = plt.subplots(figsize=(10, 6))

while True:
    if os.path.exists(parquet_output_path):
        # Load the latest Parquet data
        df = load_parquet_files(parquet_output_path)
        if not df.empty:
            # Plot the clusters
            plot_clusters(df, ax)
            print("Refreshed Real-Time URL Clustering Batch")
            plt.pause(1)  # Pause to allow the plot to update
    time.sleep(30)
