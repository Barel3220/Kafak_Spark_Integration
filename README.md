# Leveraging Big Data to Enhance Marketing Campaign Analysis

This project demonstrates the integration of Apache Kafka and Apache Spark for real-time URL clustering. The process involves producing URL data to Kafka topics, consuming the data using Spark, performing K-Means clustering, and visualizing the results.

## The problem
The current approach to analyzing marketing campaign data, which includes short text descriptions, relies heavily on traditional data analysis methods. However, these methods fall short in providing comprehensive insights necessary for making informed decisions about marketing strategies. 

The key issue is that the conventional analytics applied to short-format campaign data are inadequate, resulting in insufficient information for effective decision-making.

As a result, our project focuses on **Leveraging Big Data to Enhance Marketing Campaign Analysis.**

## Solutions (1)
#### Utilizing Big Data in Short-Text Marketing Campaign Analysis
To address insufficiencies of traditional data analysis methods for short-text marketing campaign data, incorporating Big Data strategies offers a robust solution. 

A clear understanding of Big Data, characterized by its volume, velocity, and variety, is essential for overcoming the limitations of traditional analytics. This enables the identification of relevant data sources and the design of suitable data processing pipelines, effectively addressing the shortcomings of traditional methods for short-text data analysis.

Examining Big Data applications in marketing provides valuable insights that can be adapted to improve marketing campaign analysis. By integrating these insights, you can tailor Big Data solutions to your marketing challenges. This approach can improve operational efficiency, enhance customer experiences, and support data-driven decision-making, ultimately overcoming the limitations of traditional analytics for short-text campaign data. [1]

## Solutions (2)
#### Dynamic Clustering Topic Model (DCT) for Streaming Short Documents
The Dynamic Clustering Topic Model (DCT) addresses challenges in analyzing short texts and their temporal dynamics by:

* **Dynamic Topic Modeling:** Tracking evolving topic distributions over documents and words.
* **Handling Short Texts:** Assigning a single topic to each short document and using inferred distributions for future inferences.
* **Temporal Dynamics:** Incorporating dependency models to account for topic changes over time.
* **Bayesian Approach:** Updating topic distributions with new streaming documents, balancing prior information with new evidence.

These solutions improve clustering and search tasks for short-length texts, such as social media posts, outperforming other models in perplexity and retrieval quality. [2]

## Advantages & Disadvantages
### Advantages
**Temporal Dynamics and Handling Short Text:** The DCT model effectively captures changing topic distributions over time and assigns a single topic to each short document, using prior distributions for future inferences. This approach is essential for streaming short documents where topics can shift rapidly. [2]

**Improved Clustering and Adaptability:** Experimental results show that DCT outperforms other clustering models in perplexity and retrieval quality. The Bayesian approach allows the model to update topic distributions with new streaming documents, ensuring relevancy and superior clustering performance. [2]

**Enhanced Decision-Making and Dynamic Applications:** Big Data provides deep insights, leading to more informed business decisions. In industries like marketing, it drives deeper insights and operational scale, enabling rapid response to changes in consumer preferences and market trends through real-time analysis. [1]

### Disadvantages
**Sparse and Dynamic Data:** Short documents provide limited words and change over time, making topic inference challenging. Traditional methods assume static data, which is unsuitable for dynamic streaming data. [1]

**Complexity of Inference:** Inferring latent topics in short, temporal documents is complex due to their brevity and evolving nature. [2]

**DCT Model Solution:** The DCT model addresses these challenges by tracking time-varying distributions and using prior distributions for inference, improving performance for streaming short texts like social media posts. [2]

**Big Data Challenges:** Big Data analytics require highly skilled data scientists and significant managerial adjustments. Ensuring data reliability and accuracy amid high volumes is crucial to filter useful signals from noise. [1]

## Open questions
* **Self-Adaptation to Data Streams** How can big data stream computing systems be made more self-adaptive to changes in data streams, avoiding load shedding? [3]
* **Frameworks and Analysis:** What are the most effective frameworks for processing data streams, and how do they compare? Researchers continue to explore methods for handling dynamic and evolving data in real-time. [4]
* **Campaign Performance:** How can marketers better assess the performance of their campaigns using big data? Understanding the impact of marketing efforts remains a critical question. [5]
* **Keyword Attribution:** Which keywords drive conversions? Identifying the most effective keywords for marketing optimization is an ongoing challenge. [5]

## The Project
### Data
* The database is of marketing campaigns data.
* The raw database consists of 6 columns and 32,949,122 rows.
* The columns are: 
   * Creation date, Campaign ID, Landing Page URL, Campaign title, Campaign keywords, Campaign description.
* Some of the campaign data has non-English language characters so pre-processing was required.

**The raw database:**
https://www.kaggle.com/datasets/ilana2804/campaign-data-2023/data

### Innovation in Methodology
Dynamic Clustering Topic Model (DCT) addresses the challenges of clustering short texts, like social media posts (or campaign description text), which are sparse and stream rapidly, making it difficult to infer topics over time.

The DCT model introduces a collapsed Gibbs sampling algorithm that assigns a single topic to each short document and uses past inferred topic distributions as priors, allowing for both short-term and long-term dependencies in topic evolution.

The model’s effectiveness is demonstrated through improved performance in clustering and ad-hoc search tasks against state-of-the-art methods, particularly in the context of information retrieval from Twitter datasets.

The paper’s contribution is significant in the field of text mining and information retrieval, providing a robust method for organizing and summarizing streaming short text data.

## Sources
1. Zhou, Ming, et al. "Clarifying Big Data: The Concept and Its Applications." 
Proceedings of the 2015 International Conference on Big Data Applications and Services. 2015.‏
2. Liang, Shangsong, Emine Yilmaz, and Evangelos Kanoulas. "Dynamic clustering of streaming short documents.“ Proceedings of the 22nd ACM SIGKDD international conference on knowledge discovery and data mining. 2016.‏
3. Kolajo, T., Daramola, O. & Adebiyi, A. Big data stream analysis: a systematic literature review. J Big Data 6, 47 (2019). [https://doi.org/10.1186/s40537-019-0210-7](https://doi.org/10.1186/s40537-019-0210-7)
4. Almeida, A., Brás, S., Sargento, S. et al. Time series big data: a survey on data stream frameworks, analysis and algorithms. J Big Data 10, 83 (2023). [https://doi.org/10.1186/s40537-023-00760-1](https://doi.org/10.1186/s40537-023-00760-1)
5. [Big Data For Marketing: The Ultimate Rundown @ marketinginsidergroup.com] (https://marketinginsidergroup.com/content-marketing/big-data-for-marketing-its-all-about-the-questions/)

## Prerequisites

- Java 8 or higher
- Apache Kafka
- Apache Spark
- Python 3.x
- Required Python packages (listed in `requirements.txt`)

## Setup

### Step 1: Download and Install Kafka

1. **Download Kafka**: 
   - Go to the [Kafka Download Page](https://kafka.apache.org/downloads) and download the latest version.

2. **Extract Kafka**:
   ```sh
   tar -xzf kafka_2.13-2.8.0.tgz
   cd kafka_2.13-2.8.0
   ```

### Step 2: Start Zookeeper and Kafka Server

1. **Start Zookeeper**:
    ```sh
   bin/zookeeper-server-start.sh config/zookeeper.properties
    ```
   
2. **Start Kafka Server**:
    ```sh
   bin/kafka-server-start.sh config/server.properties
   ```

### Step 3: Create Kafka Topics

Use the provided script create_topics.py to create Kafka topics. Run the script with:
```sh
python create_topics.py
```

### Step 4: Prepare Data

Ensure you have your data file sample_campaign_data_2023.csv. This file is already preprocessed!
The preprocessing.py file is for larger files (millions of rows)

### Step 5: Create Required Directories

Ensure the directories for Spark output and checkpoints exist:
```sh
mkdir -p tmp/output
mkdir -p tmp/checkpoint
```
### Step 6: Run the Spark Consumer and Clustering

Use the script spark_consumer_producer.py to start the Spark job. Submit the job with:
```sh
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 spark_consumer_producer.py
```

### Step 7: Run the Kafka Consumers

Use the script multiple_consumers.py to start the consumers job. Submit the job with:
    
```sh
python multiple_consumers.py
```

### Step 8: Visualize Real-Time URL Clustering

Use the script realtime_visualization_topics.py to see the clustering results in real-time:
```sh
python realtime_visualization_topics.py
```

### Step 9: Visualize Message Counts

Run the visualization script realtime_visualization_messages.py to see the message counts over time for each cluster:
```sh
python realtime_visualization_messages.py
```

### Step 10: Produce Data to Kafka

Use the producer script producer.py to send data to Kafka:
```sh
python producer.py
```

## Requirements
Create a requirements.txt file with the following content:
```sh
confluent_kafka
pandas
matplotlib
seaborn
tldextract
pyspark
```
Install the required Python packages with:
```sh
pip install -r requirements.txt
```

## Project Structure

- create_topics.py: Script to create Kafka topics.
- preprocessing.py: Script to preprocess the input data.
- producer.py: Script to produce data to Kafka topics.
- spark_consumer_producer.py: Script to consume data from Kafka, perform K-Means clustering using Spark, and produce clustered data back to Kafka.
- realtime_visualization_topics.py: Script to visualize the real-time URL clustering.
- realtime_visualization_messages.py: Script to visualize the message counts over time for each cluster.

## Notes

- Ensure Kafka and Zookeeper are running before starting the producer and consumer scripts.
- Adjust the paths in the scripts according to your project directory structure.

# Example of plots from Real-Time run:
## Messages Over Time:
![Sample Output](messages_over_time.png)

## Clusters Over Time:
![Sample Output](clusters_over_time.png)

