# Tesey DeltaIngester

> Tesey DeltaIngester can be used to ingest data from JDBC sources to HDFS and object storages like s3, gs, etc.

## Usage

1. Clone the repository, and package with:

```
mvn clean install
```

2. Describe endpoints configs in endpoints.json similar to following:
```json
{
    "endpoints": [
        {
            "name": "test-db",
            "options": [
                {"name": "host", "value": "testdb"},
                {"name": "port", "value": "1521"},
                {"name": "dbName", "value": "test"},
                {"name": "dbType", "value": "oracle"},
                {"name": "user", "value": "root"},
                {"name": "credentialProviderPath", "value": "jceks://hdfs/user/hadoop/test-root-key.jceks"},
                {"name": "passwordAlias", "value": "oracle.password.alias"}
            ]
        },
        {
            "name": "test-parquet",
            "options": [
                {"name": "location", "value": "hdfs:///test/parquet"},
                {"name": "format", "value": "parquet"}
            ]
        }
    ]
}
```

3. Prepare Avro schemas corresponding with schemas of tables that should be ingested.

4. Describe tables configs in tables.json similar to following:
```json
{
    "tables": [
        {
            "name" : "test",
            "options" : [
                {"name": "schema", "value": "test.avsc"},
                {"name": "tableName", "value": "test"},
                {"name": "mode", "value": "daily"},
                {"name": "checkField", "value": "load_date"},
                {"name": "partitionKeys", "value": "load_date"}
            ]
        }
    ]
}
```

5. Submit Spark Application like the following:
```shell script
spark-submit \
--class org.tesey.ingester.spark.DeltaIngester \
--name DeltaIngester \
--master yarn \
--num-executors 2 \
--driver-memory 512m \
--executor-memory 512m \
-m yarn-cluster target/tesey-delta-ingester-1.0-SNAPSHOT.jar \
--endpointsConfigPath hdfs:///configs/endpoints.json \
--tablesConfigPath hdfs:///configs/tables.json \
--schemasPath hdfs:///schemas \
--source-name test-db \
--sink-name test-parquet \
--mode daily
```
