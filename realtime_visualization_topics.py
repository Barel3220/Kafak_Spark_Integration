import os
import sys
import time

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Define the base path for reading Parquet files
base_path = os.path.dirname(os.path.abspath(sys.argv[0]))

# Define the path for the Parquet output files
parquet_output_path = os.path.join(base_path, "tmp/output")


def load_parquet_files(path):
    """Loads all Parquet files in the specified directory.

    Args:
        path (str): The directory path containing Parquet files.

    Returns:
        DataFrame: A DataFrame containing the loaded data.
    """
    return pd.read_parquet(path)


def plot_clusters(df_, ax_):
    """Plots clusters from the DataFrame on the provided axes.

    Args:
        df_ (DataFrame): The DataFrame containing the data to plot.
        ax_ (Axes): The matplotlib axes to plot on.
    """
    ax_.clear()  # Clear the previous plot
    sns.scatterplot(data=df_, x='main_domain', y='cluster', hue='cluster', palette='tab10', ax=ax_)
    ax_.set_title('Real-Time URL Clustering')
    ax_.set_xlabel('Main Domain')
    ax_.set_ylabel('Cluster')


fig, ax = plt.subplots(figsize=(10, 6))

while True:
    if os.path.exists(parquet_output_path):
        # Load the latest Parquet data
        df = load_parquet_files(parquet_output_path)
        if not df.empty:
            plot_clusters(df, ax)
            print("Refreshed Real-Time URL Clustering Batch")
            plt.pause(10)  # Pause to allow the plot to update
    time.sleep(30)
