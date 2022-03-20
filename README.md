<h1>BDA Assignment 3: Spark Streaming</h1>

This project scrapes all forum posts as well as their comments in Hardwarezone.

We would be using Scrapy to scrape the data before publishing them over to kafka to a topic called: 'scrapy-output'. 

Then we would use Spark Streaming to consume the data from the 'scrapy-output' topic from kafka. 

Spark Streaming will then output the top 10 users and top 10 words in every 2 minutes windows

<h2>Dependencies</h2>

1. Kafka
2. Zookeeper
3. Scrapy
4. Spark
5. PySpark

<h2>Steps on how to run the project</h2>

1. Start Zookeeper on a separate terminal
```
zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties
```

2. Start Kafka on a terminal
```
kafka-server-start /usr/local/etc/kafka/server.properties
```

3. On another terminal run the scrapy spider
```
cd BDA_assignment3/hardwarezone/spiders
scrapy runspider spider.py
```

4. Lastly, on another terminal run kafka_wordcount.py
```
cd BDA_assignment3
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 kafka_wordcount.py
```

<h2>Sample Output</h2>

# Top 10 Words within last 2 minute window
![Alt text](/sample_output/top_words.png?raw=true "Top 10 words within last 2 minute window")

# Top 10 Users within last 2 minute window
![Alt text](/sample_output/top_users.png?raw=true "Top 10 users within last 2 minute window")
