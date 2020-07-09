# Tesey DeltaIngester

Tesey DeltaIngester can be used to ingest data from JDBC sources to HDFS and object storages like s3, gs, etc.

## Usage

1. Clone the repository, and package with:

```
mvn clean install
```

2. Describe <a href="#deltaingester.io/EndpointsSpecification">endpoints</a> configs in endpoints.json similar to following:
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
                {"name": "driver", "value": "oracle.jdbc.driver.OracleDriver"},
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

4. Describe <a href="#deltaingester.io/TablesSpecification">tables</a> in tables.json similar to following:
```json
{
    "tables": [
        {
            "name" : "test",
            "options" : [
                {"name": "tableName", "value": "test"},
                {"name": "schema", "value": "test.avsc"},
                {"name": "mode", "value": "daily"},
                {"name": "checkField", "value": "load_date"},
                {"name": "partitionKeys", "value": "load_date"}
            ]
        }
    ]
}
```

5. Submit <a href="#deltaingester.io/SparkApplicationArguments">Spark Application</a> like the following:
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

<h2 id="deltaingester.io/EndpointsSpecification">Endpoints specification
</h2>
<table>
<thead>
<tr>
<th>Field</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>url</code></br>
string</td>
<td>
Database connection URL
</td>
</tr>
<tr>
<td>
<code>host</code></br>
string</td>
<td>
The host name of source database server
</td>
</tr>
<tr>
<td>
<code>port</code></br>
string</td>
<td>
The port of source database server
</td>
</tr>
<tr>
<td>
<code>dbName</code></br>
string</td>
<td>
The name of source database
</td>
</tr>
<tr>
<td>
<code>dbType</code></br>
string</td>
<td>
The name of RDBMS. Currently supported `oracle`
</td>
</tr>
<tr>
<td>
<code>driver</code></br>
string</td>
<td>
Database driver
</td>
</tr>
<tr>
<td>
<code>user</code></br>
string</td>
<td>
The name of user to connect to source database
</td>
</tr>
<tr>
<td>
<code>credentialProviderPath</code></br>
string</td>
<td>
The path to <a href="https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/CredentialProviderAPI.html">credential store provider</a> that is used to retrieve the password of user to connect to source database
</td>
</tr>
<tr>
<td>
<code>passwordAlias</code></br>
string</td>
<td>
The <a href="https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/CredentialProviderAPI.html">credential alias</a> used to retrieve password of user to connect to source database
</td>
</tr>
<tr>
<td>
<code>location</code></br>
string</td>
<td>
The path to write the ingested data
</td>
</tr>
<tr>
<td>
<code>format</code></br>
string</td>
<td>
The format to save the ingested data. Currently supported types:

* AVRO
* Parquet
* ORC
</td>
</tr>
</tbody>
</table>

<h2 id="deltaingester.io/TablesSpecification">Tables specification
</h2>
<table>
<thead>
<tr>
<th>Field</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>tableName</code></br>
string</td>
<td>
The name of ingesting table in source database
</td>
<tr>
<td>
<code>schema</code></br>
string</td>
<td>
The path to Avro schema, that corresponds with the structure of ingesting rows
</td>
</tr>
<tr>
<td>
<code>mode</code></br>
string</td>
<td>
The ingestion mode. The possible options:

* completely - ingesting all rows from source table
* incrementally - ingesting rows where check column has a value greater than the one specified with `lastValue`
* daily - ingesting rows from source table inserted in previous day 
</td>
</tr>
<tr>
<td>
<code>checkField</code></br>
string</td>
<td>
The check column used to identify rows that should be ingested in modes `incrementally` and `daily`
</td>
</tr>
<tr>
<td>
<code>lastValue</code></br>
string</td>
<td>
The maximum value of check column in the previous ingestion, used to indentify rows that should be ingested in mode `incrementally`
</td>
</tr>
<tr>
<td>
<code>partitionKeys</code></br>
string</td>
<td>
A comma-separated list of fields which is used for partitioning the output datasets on
</td>
</tr>
</tbody>
</table>

<h2 id="deltaingester.io/SparkApplicationArguments">Spark Application arguments
</h2>
<table>
<thead>
<tr>
<th>Field</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>endpointsConfigPath</code></br>
string</td>
<td>
The path to endpoints config file
</td>
</tr>
<tr>
<td>
<code>tablesConfigPath</code></br>
string</td>
<td>
The path to tables config file
</td>
</tr>
<tr>
<td>
<code>schemasPath</code></br>
string</td>
<td>
The path to Avro schemas
</td>
</tr>
<tr>
<td>
<code>sourceName</code></br>
string</td>
<td>
The name of endpoint that is used as a data source
</td>
</tr>
<tr>
<td>
<code>sinkName</code></br>
string</td>
<td>
The name of endpoint that is used as a data sink
</td>
</tr>
<tr>
<td>
<code>mode</code></br>
string</td>
<td>
The ingestion mode (completely/daily/incrementally)
</td>
</tr>
</tbody>
</table>
