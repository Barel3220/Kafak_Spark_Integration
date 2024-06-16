import json
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import pandas as pd


def read_message_counts(input_file='message_counts.json'):
    try:
        with open(input_file, 'r') as f:
            data = json.load(f)
        return data['counts'], data['clusters']
    except FileNotFoundError:
        return [], []


def animate(i, xs_, ys_):
    counts, clusters = read_message_counts()
    if counts and clusters:
        xs_.clear()
        ys_.clear()
        xs_.extend(clusters)
        ys_.extend(counts)
        ax.clear()

        # Create a dataframe with appropriate lengths
        df = pd.DataFrame({'Cluster': xs_, 'Count': ys})

        # Plot the data
        df.plot(kind='bar', x='Cluster', y='Count', ax=ax)

        plt.xticks(rotation=45, ha='right')
        plt.subplots_adjust(bottom=0.30)
        plt.title('Message Rate Over Time - Total: ' + str(sum(counts)))
        plt.ylabel('Messages Counts')
        plt.xlabel('Clusters')


if __name__ == "__main__":
    fig, ax = plt.subplots()
    XS = []
    ys = []

    ani = animation.FuncAnimation(fig, animate, fargs=(XS, ys), interval=1000)
    plt.show()
