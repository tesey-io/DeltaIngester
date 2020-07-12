/*
 * Copyright 2020 The Tesey Software Authors
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.tesey.spark.ingester

import org.tesey.spark.ingester.meta._
import com.databricks.spark.avro.SchemaConverters
import com.typesafe.scalalogging.Logger
import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StructField, StructType}

import scala.collection.JavaConverters._
import java.net.URI
import java.time.format.DateTimeFormatter
import java.time.{ZoneId, ZonedDateTime}
import java.util.Properties

package object processing {

  val DEFAULT_DATE_FORMAT = "yyyy-MM-dd"

  def processTable(sparkSession: SparkSession, mode: String, format: String, location: String, schemasLocation: String,
                   sourceConfig: ConfigItem, sinkConfig: ConfigItem, credentials: Properties, logger: Option[Logger] = None)
                  (tableOptions: List[ConfigOption]): Unit = {

    val tableName = getOptionValueFromConfig("tableName", tableOptions, logger)

    // Read rows
    var result = ingestTable(sparkSession, tableName, mode, credentials, sourceConfig, tableOptions)

    val formatString = format match {
      case "avro" => "com.databricks.spark.avro"
      case _ => format
    }

    val writeMode = mode match {
      case "completely" => "overwrite"
      case _ => "append"
    }

    val tableSchemaLocation = getOptionFromConfig("schema", tableOptions)

    if (tableSchemaLocation.isDefined) {

      val fileSystem = FileSystem.get(new URI(schemasLocation), new Configuration())

      val schema: Schema = new Schema.Parser()
        .parse(fileSystem.open(new Path(s"$schemasLocation/${tableSchemaLocation.get.value}")))

      val sparkSqlSchema: StructType = mapAvroToSparkSqlTypes(schema)

      val fields: Seq[Column] = sparkSqlSchema.map(f => col(f.name).cast(f.dataType))

      result = result.select(fields:_*)

    }

    var resultWriter = result.write.format(formatString).mode(writeMode)

    val partitionKeys = getOptionFromConfig("partitionKeys", tableOptions)

    if (partitionKeys.isDefined) {
      resultWriter = resultWriter.partitionBy(partitionKeys.get.value)
    }

    // Write rows
    resultWriter.save(s"$location/$tableName")

  }

  def mapAvroToSparkSqlTypes(schema: Schema, nullable: Boolean = true): StructType =
    new StructType(
      schema.getFields.asScala.toList.map(f => {
        StructField(
          f.name(),
          SchemaConverters.toSqlType(f.schema()).dataType,
          nullable = nullable
        )
      }).toArray
    )

  def ingestTable(sparkSession: SparkSession, tableName: String, mode: String, credentials: Properties,
                  sourceConfig: ConfigItem, tableOptions: List[ConfigOption], logger: Option[Logger] = None): DataFrame = {

    val sourceOptions = sourceConfig.options.get

    val dbTypeOption = getOptionFromConfig("dbType", sourceOptions)

    if (!dbTypeOption.isDefined) {

      if (logger.isDefined) {

        logger.get.error(s"Error: Please specify option 'dbType' for source '${sourceConfig.name}'")

      }

      System.exit(1)

    }

    val dbType = dbTypeOption.get.value

    val dbUrl = getOptionFromConfig("url", sourceOptions)

    var jdbcConnectionString = ""

    if (dbUrl.isDefined) {

      jdbcConnectionString = dbUrl.get.value

    } else {

      val host = getOptionFromConfig("host", sourceOptions)

      val port = getOptionFromConfig("port", sourceOptions)

      val dbName = getOptionFromConfig("dbName", sourceOptions)

      if (host.isDefined && port.isDefined && dbName.isDefined) {

        jdbcConnectionString = dbType match {
          case "oracle" => s"jdbc:oracle:thin:/@${host.get.value}:${port.get.value}/${dbName.get.value}"
          case _ => ""
        }

      }  else {

        if (logger.isDefined) {

          logger.get
            .error(s"Error: Please specify options 'host', 'port' and 'dbName' for source '${sourceConfig.name}'")

        }

        System.exit(1)

      }

    }

    val query = mode match {
      case "incrementally" => getQueryToIngestIncrementally(tableName,
        getOptionValueFromConfig("checkColumn", tableOptions, logger),
        getOptionValueFromConfig("lastValue", tableOptions, logger))
      case "daily" => getQueryToIngestDaily(tableName,
        getOptionValueFromConfig("checkColumn", tableOptions, logger), dbType)
      case _ => s"${tableName}"
    }

    val driver = getOptionValueFromConfig("driver", sourceOptions)

    var options = dbType match {
      case "oracle" => Map(
        "sessionInitStatement"           -> "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD'",
        "sessionInitStatement"           -> "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF6'",
        "oracle.jdbc.mapDateToTimestamp" -> "false",
        "isolationLevel"                 -> "READ_COMMITTED"
      )
      case _ => Map("" -> "")
    }

    val partitionColumn = getOptionFromConfig("partitionColumn", tableOptions)

    val lowerBound = getOptionFromConfig("lowerBound", tableOptions)

    val upperBound = getOptionFromConfig("upperBound", tableOptions)

    val numPartitions = getOptionFromConfig("numPartitions", tableOptions)

    if (partitionColumn.isDefined && lowerBound.isDefined && upperBound.isDefined && numPartitions.isDefined) {

      options = options ++ Map(
        "partitionColumn" -> partitionColumn.get.value,
        "lowerBound" -> lowerBound.get.value,
        "upperBound" -> upperBound.get.value,
        "numPartitions" -> numPartitions.get.value
      )

    }

    val tableBatchSize = getOptionFromConfig("batchSize", tableOptions)

    val sourceBatchSize = getOptionFromConfig("batchSize", sourceOptions)

    if (tableBatchSize.isDefined) {
      options = options ++ Map(
        "batchsize" -> tableBatchSize.get.value
      )
    } else if (sourceBatchSize.isDefined) {
      options = options ++ Map(
        "batchsize" -> sourceBatchSize.get.value
      )
    }

    options = options ++ Map("driver" -> driver)

    sparkSession.read
      .options(options)
      .jdbc(
        jdbcConnectionString,
        query,
        credentials
      )

  }

  def getQueryToIngestDaily(tableName: String, checkColumn: String, dbType: String): String = {

    val prevDate = ZonedDateTime.now(ZoneId.of("UTC")).minusDays(1)

    val formatter = DateTimeFormatter.ofPattern(DEFAULT_DATE_FORMAT)

    val prevDateString = formatter format prevDate

    val checkColumnValue = dbType match {
      case "oracle" => s"TO_DATE('$prevDateString','$DEFAULT_DATE_FORMAT')"
      case _        => s"'$prevDateString'"
    }

    s"(SELECT * FROM $tableName WHERE $checkColumn = $checkColumnValue)"
  }

  def getQueryToIngestIncrementally(tableName: String, checkColumn: String, lastValue: String): String = {

    s"(SELECT * FROM $tableName WHERE $checkColumn >= $lastValue)"

  }

}
