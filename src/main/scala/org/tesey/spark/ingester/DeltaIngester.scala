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
import org.tesey.spark.ingester.parser._
import org.tesey.spark.ingester.processing._
import org.apache.spark.sql.SparkSession
import com.typesafe.scalalogging.slf4j.LazyLogging

object DeltaIngester extends App with LazyLogging {

  val OPTIONS = Map(
    "endpointsConfigPath" -> ("Path to endpoints config file", "<endpointsConfigPath>"),
    "tablesConfigPath"    -> ("Path to tables config file", "<tablesConfigPath>"),
    "schemasPath"         -> ("Path to Avro schemas", "<tablesConfigPath>"),
    "sourceName"          -> ("Name of endpoint that is used as a data source", "<sourceName>"),
    "sinkName"            -> ("Name of endpoint that is used as a data sink", "<sinkName>"),
    "mode"                -> ("Ingestion mode (completely/daily/incrementally)", "<mode>")
  )

  def parseArgs(argList: List[String], argMap: Map[String, String] = Map()): Map[String, String] = {
    argList match {
      case arg :: args if arg.startsWith("--") => parseArgs(
        argList.tail,
        argMap ++ Map(arg.replaceAll("--", "") -> args.head)
      )
      case Nil => argMap
    }
  }

  override def main(args: Array[String]): Unit = {

    lazy val options: Map[String, String] = parseArgs(argList = args.toList)

    val endpointsConfig = new ConfigParser[EndpointsConfig](options("endpointsConfigPath")).config

    val tablesConfig = new ConfigParser[TablesConfig](options("tablesConfigPath")).config

    val mode = options("mode")

    val schemasLocation = options("schemasPath")

    val sourceName = options("sourceName")

    val sinkName = options("sinkName")

    val sparkSessionBuilder = SparkSession.builder().appName("JdbcIngester")
      .config("spark.hadoop.avro.mapred.ignore.inputs.without.extension", "false")


    // Read configs
    val sourceConfig = endpointsConfig.endpoints.find(_.name == sourceName).get

    val sinkConfig = endpointsConfig.endpoints.find(_.name == sinkName).get

    val sparkSession = sparkSessionBuilder.getOrCreate()

    // Extract credentials
    val credentialProviderPath = getOptionFromConfig("credentialProviderPath", sourceConfig.options.get)

    val user = getOptionFromConfig("user", sourceConfig.options.get)

    val passwordAlias = getOptionFromConfig("alias", sourceConfig.options.get)

    if (credentialProviderPath.isDefined && user.isDefined && passwordAlias.isDefined) {

      val credentials = extractCredentials(user.get.value, passwordAlias.get.value, credentialProviderPath.get.value)

      val format = getOptionFromConfig("format", sinkConfig.options.get)

      val location = getOptionFromConfig("location", sinkConfig.options.get)

      if (format.isDefined && location.isDefined) {

        // Process tables
        val tableProcessor = processTable(sparkSession, mode, format.get.value, location.get.value, schemasLocation,
          sourceConfig, sinkConfig, credentials, Some(logger)) _

        tablesConfig.tables
          .filter(m => getOptionFromConfig("mode", m.options.get).get.value == mode)
          .foreach(i => {
            tableProcessor(i.options.get)
          })

      }  else {

        logger.error(s"Error: Please specify options 'format' and 'location' for source '$sourceName'")

        System.exit(1)

      }

    } else {

      logger.error(s"Error: Please specify options 'user', 'alias' and 'credentialProviderPath' for source '$sourceName'")

    }

  }

}
