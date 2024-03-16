# Reference examples to make spark pipelines

## Spin up the docker compose network

```sh 
docker compose -f ./spark-docker.yml -d up
```

## Build the project

```sh 
cd ./spark-data-engg/
mvn package 
# Load the database 
mvn exec:java -Dexec.mainClass="com.learning.sparkdataengg.setup.SetupPrerequisites"
# load the database 
mvn exec:java -Dexec.mainClass="com.learning.sparkdataengg.chapter3.RawStockDataGenerator"

```
