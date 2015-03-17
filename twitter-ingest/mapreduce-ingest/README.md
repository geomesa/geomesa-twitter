# Mapreduce Jobs to Ingest Twitter

### Example
    hadoop jar twitter-ingest-mapreduce-1.0-SNAPSHOT.jar geomesa.example.twitter.ingest.mapreduce.TwitterIngest \
    -u user -p pass -i mycloud -z zoo1,zoo2,zoo3 -f twitter -c mycatalog /in/twitter/json/2015/03/03/*