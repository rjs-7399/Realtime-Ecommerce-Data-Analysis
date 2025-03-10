## Ecommerce Data Analytics

- Changes that are made for this project.
- We have changed the java version from JDK 22 to 11. Because Java 11 is stable and other versions might give some compatibility issues.
- We have set the dependencies into pom.xml.
- We have installed flink-1.18.0 version.
- Here the steps to set up and install java and flink are added in this readme.

### Commands for kafka message generation

- Run docker compose in terminal 1

```commandline
docker compose up -d
```

- Run python producer in terminal 2

```commandline
python3 producer_script.py
```

- Run broker container in terminal 3

```commandline
docker exec -it broker bash
```
```
# inside broker container
kafka-topics --list --bootstrap-server broker:29092
sales_transactions


kafka-console-consumer --topic sales_transactions --bootstrap-server localhost:9092

{"transactionId": "cd3fa541-9812-4dd7-b73b-49e6fef85e79", "productId": "product3", "productName": "headphone", "productCategory": "sports", "productPrice": 863.38, "productQuantity": 3, "productBrand": "samsung", "currency": "GBP", "customerId": "brownkristen", "transactionDate": "2025-02-21T06:40:433546+0000", "paymentMethod": "credit_card", "totalAmount": 2590.14}
{"transactionId": "e4457989-df7a-47b8-82cf-118c9c406897", "productId": "product5", "productName": "watch", "productCategory": "home", "productPrice": 785.7, "productQuantity": 2, "productBrand": "apple", "currency": "GBP", "customerId": "cwillis", "transactionDate": "2025-02-21T06:45:439499+0000", "paymentMethod": "debit_card", "totalAmount": 1571.4}

```


# Java 11 Setup Guide for Mac

This guide walks through setting up Java 11 for your Flink project on macOS.

## Installation Steps

### 1. Install Homebrew (if not already installed)
```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

### 2. Install Java 11
```bash
brew tap adoptopenjdk/openjdk
brew install --cask adoptopenjdk11
```

### 3. Configure Java Environment

Add these lines to your `~/.zshrc`:
```bash
# Java 11 configuration
export JAVA_HOME=$(/usr/libexec/java_home -v 11)
export PATH=$JAVA_HOME/bin:$PATH
```

Reload your shell configuration:
```bash
source ~/.zshrc
```

### 4. Verify Java Installation
```bash
java --version
```
This should show Java 11 as your current version.

## Project Configuration

### 5. Update pom.xml

Update the Java version in your project's `pom.xml`:

```xml
<properties>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    <flink.version>1.18.0</flink.version>
    <target.java.version>11</target.java.version>
    <scala.binary.version>2.12</scala.binary.version>
    <maven.compiler.source>${target.java.version}</maven.compiler.source>
    <maven.compiler.target>${target.java.version}</maven.compiler.target>
    <log4j.version>2.17.1</log4j.version>
</properties>
```

### 6. Rebuild Project

Clean and rebuild your Maven project:
```bash
mvn clean
mvn package
```

## Troubleshooting

If you encounter issues:

1. Verify Java version:
```bash
/usr/libexec/java_home -V
```
This shows all installed Java versions.

2. Check JAVA_HOME setting:
```bash
echo $JAVA_HOME
```

3. If you have multiple Java versions, you can switch between them by modifying the version number in your JAVA_HOME export statement:
```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 11)
```

## Notes
- Make sure to restart your IDE after changing Java versions
- If using IntelliJ, you may need to update the project SDK settings (File -> Project Structure -> Project SDK)
- The Flink version 1.18.0 is compatible with Java 11


# Apache Flink 1.18.0 Setup Guide for macOS

This guide provides step-by-step instructions for downloading and setting up Apache Flink 1.18.0 on macOS.

## Prerequisites

- macOS operating system
- Java 8 or later installed
- Terminal access

## Check Java Installation

Before proceeding, verify your Java installation by running:
```bash
java -version
```

## Installation Steps

### 1. Download Flink

You can download Flink using either wget or curl:

Using wget:
```bash
wget https://archive.apache.org/dist/flink/flink-1.18.0/flink-1.18.0-bin-scala_2.12.tgz
```

Or using curl:
```bash
curl -O https://archive.apache.org/dist/flink/flink-1.18.0/flink-1.18.0-bin-scala_2.12.tgz
```

### 2. Extract the Archive

Extract the downloaded file:
```bash
tar -xzf flink-1.18.0-bin-scala_2.12.tgz
```

### 3. Navigate to Flink Directory

Change to the Flink directory:
```bash
cd flink-1.18.0
```

### 4. Start Flink Cluster

Start the Flink cluster using the provided script:
```bash
./bin/start-cluster.sh
```

### 5. Verify Installation

1. Open your web browser
2. Navigate to http://localhost:8081
3. You should see the Flink Web UI dashboard

## Stopping the Cluster

To stop the Flink cluster when you're done:
```bash
./bin/stop-cluster.sh
```

## Optional Configuration

### Add Flink to PATH

Add Flink to your system PATH by adding this line to your ~/.bash_profile or ~/.zshrc:
```bash
export PATH=$PATH:/path/to/flink-1.18.0/bin
```
Replace "/path/to" with your actual Flink installation path.

Remember to source your profile file after making changes:
```bash
source ~/.bash_profile  # or source ~/.zshrc
```

## Troubleshooting

If you encounter any issues:
1. Ensure Java is properly installed and JAVA_HOME is set
2. Check if the required ports (8081 for Web UI) are available
3. Review the Flink logs in the `log` directory of your Flink installation
4. We have added some changes into conf/flink-conf.yaml file to make it effective with our project.
5. taskmanager.numberOfTaskSlots: 4
6. parallelism.default: 2
7. Once we spin-up the flink server we can see all the configuration and settings available on `localhost:8081`
8. Now As we have a java project to run the flink we are going to follow the java standards.
9. We need to run these steps.
10. mvn clean
11. mvn compile
12. mvn package
13. After that it will build the jar file of entire java project. Every time we need to supply this jar file at the run time.
14. To run the DataStream Object we need to run this command `/Users/rutvikshah/Desktop/data_engineering/data_engineering_setup/flink-1.18.0/bin/flink run -c FlinkCommerce.DataStreamJob target/FlinkCommerce-1.0-SNAPSHOT.jar`
15. With this command we need to run the docker compose up -d in one terminal, in second terminal we can run the python producer to produce the data into kafka topic and in third terminal we can run the flink job. So this will show that data is getting printed on UI.
16. Here we simply deserialized our data from kafka producer into flink job. We are eventually consume the data from kafka producer into flink job.
17. So next we are going to set up the connection between flink stream and postgres.
18. And then we are going to sync the data into elasticsearch.
19. So here we are storing real-time data coming from kafka topic into postgres.
20. One thing that we can say is, pyspark structured streaming is a micro-batching approach.
21. In structured streaming, a large set of data is batched in a very small size of batch and that batched data is getting inserted into the sink at a give duration.
22. So pyspark structured streaming is not accurately real-time, But it's near real time process.
23. But that is not the case with flink. Flink copies the data from source to sink at a real time. No micro-batching patterns in flink.
24. So in this project we are taking the data from kafka source. Flink processes the data and dumps it into the postgres which is playing a role of data warehouse.
25. So in postgres data, we will have a data being developed in day, month and category. So we will have sales per day month and category.

## Additional Resources

- [Apache Flink Documentation](https://nightlies.apache.org/flink/flink-docs-release-1.18/)
- [Flink Operations UI Documentation](https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/ops/monitoring/webfrontend/)