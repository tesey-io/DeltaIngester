package org.tesey.spark.ingester

import com.typesafe.scalalogging.Logger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.alias.CredentialProviderFactory

import java.util.Properties

package object meta {

  case class EndpointsConfig(endpoints: List[ConfigItem])

  case class TablesConfig(tables: List[ConfigItem])

  case class ConfigItem(name: String, options: Option[List[ConfigOption]])

  case class ConfigOption(name: String, value: String) {
    val optionMap: (String, String) = name -> value
  }

  def getOptionFromConfig(name: String, options: List[ConfigOption], logger: Option[Logger] = None): Option[ConfigOption] = {

    val configOption = options.find(o => o.name == name)

    configOption

  }

  def getOptionValueFromConfig(name: String, options: List[ConfigOption], logger: Option[Logger] = None): String = {

    val configOption = getOptionFromConfig(name, options)

    if (!configOption.isDefined) {

      logger.get.error(s"Config option ${name} not found")

      System.exit(1)

    }

    configOption.get.value

  }

  def extractCredentials(login: String, passwordAlias: String, providerPath: String): Properties = {

    val hadoopConf = new Configuration()

    hadoopConf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, providerPath)

    val properties = new Properties()

    properties.put("user", login)

    properties.put("password", hadoopConf.getPassword(passwordAlias).mkString)

    properties

  }

}
