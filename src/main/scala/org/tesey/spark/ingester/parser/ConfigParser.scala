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
