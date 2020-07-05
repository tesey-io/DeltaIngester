package org.tesey.spark.ingester.parser

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods.parse

import java.net.URI

class ConfigParser[T] (val uri: String) (implicit m: Manifest[T]) {

  val fileSystem: FileSystem = FileSystem.get(new URI(uri), new Configuration())

  val inputStream = fileSystem.open(new Path(uri))

  implicit val format: DefaultFormats.type = DefaultFormats

  val config = parse(inputStream).extract[T]

}
