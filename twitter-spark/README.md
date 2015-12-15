to run stalker:

    bin/spark-submit  ~/twitter-spark-1.0.0-SNAPSHOT.jar  -u <user> -p <pass> -c <mycatalog> -i <inst> -z <zoo> -fn twitter --min-count 2 --output-dir /temp/ --geo-hash-bits 25
