# Twitter Spark Streaming Data using Java, Spark Streaming, Kafka, HBase 
BDT project using Spark Streaming to analyze popular hashtags from twitter.
The data comes from the Twitter Streaming API source and is fed to Kafka.
The consumer com.twitter.producer.service receives data from Kafka and then 
processes it in a stream using Spark Streaming then save it into HBase.


## Requirements
- JVM at least version 8
- Apache Maven 3.x or later
- Install Spark and Zookeeper
- Docker container
- Kafka
- HBase
- Twitter API. 

## How to Run the application
- Change Twitter configuration in `\producer\src\main\resources\application.yml` with your API Key, client Id and Secret Id.

- Run the kafka image 

```
~> docker-compose -f producer/src/main/docker/kafka-docker-compose.yml up -d   
```

- Check if ZooKeeper and Kafka is running

```
~> docker ps 
```

- Run poducer and consumer app with:
```
~> mvn spring-boot:run
```
- Open the HBase Shell to see check if is storing
```
~> hbase shell
~> list
~> scan "SparkStreaming_project"
```
