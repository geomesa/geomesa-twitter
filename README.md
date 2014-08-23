## GeoMesa Twitter

GeoMesa Twitter provides three example projects to collect, ingest, and analyze Twitter data in GeoMesa.

## Getting Started

You'll first need to clone and build the GeoMesa 1.0.0-SNAPSHOT from <a href="https://github.com/locationtech/geomesa">LocationTech GeoMesa Github</a>.

Checkout, build, and install GeoMesa with maven:

```
git clone git@github.com:locationtech/geomesa.git
cd geomesa
mvn clean install
```

Next clone geomesa-twitter and build geomesa-twitter:

```
git clone git@github.com:geomesa/geomesa-twitter.git
cd geomesa-twitter
mvn clean install
```

## Collected Twitter Data
Sign up for twitter and get developer keys...more info coming soon

## Ingesting Twitter into GeoMesa

Run the ingest jar to see a list of arguments for ingest:

```
$> java -jar twitter-ingest/target/twitter-ingest-accumulo1.5-1.0-SNAPSHOT.jar 
0 [main] INFO geomesa.example.twitter.ingest.Runner  - Error parsing arguments: The following options are required: --instanceId, -i --zookeepers, -z --tableName, -t --password, -p --user, -u --featureName, -f 
Usage: <main class> [options] files
  Options:
  * --featureName, -f
       featureName to assign to the data
        --indexSchemaFormat
       Schema for indexing data
  * --instanceId, -i
       Name of the Accumulo Instance
  * --password, -p
       Accumulo password
    --shards, -s
       number of shards to use for data
  * --tableName, -t
       Accumulo table name
        --useExtendedFeatures
       parse extended features or the minimal set
  * --user, -u
       Accumulo user name
  * --zookeepers, -z
       Comma separated list of zookeepers
```

After collecting some twitter data, you can ingest files which have one tweet as JSON per line:

```java -jar twitter-ingest/target/twitter-ingest-accumulo1.5-1.0-SNAPSHOT.jar -u user -p password -z zoo1,zoo2,zoo3 -t geomesa_catalog -i my instance -f twitter tweet_file1.json tweet_file2.json tweet_file3.json```

## Running the Spark Job
After setting up spark on your cluster, copy the shade jar over and run it:

```
/opt/spark/bin/spark-submit --master yarn-client --num-executors 40 --executor-cores 4 twitter-spark-accumulo1.5-1.0-SNAPSHOT.jar  --deploy-mode client --class geomesa.example.twitter.spark.Runner twitter
```

