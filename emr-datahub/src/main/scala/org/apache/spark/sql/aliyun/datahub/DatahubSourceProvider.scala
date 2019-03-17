/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.aliyun.datahub

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.StructType

class DatahubSourceProvider extends DataSourceRegister with StreamSourceProvider{
  override def shortName(): String = "datahub"

  override def sourceSchema(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = {
    (shortName(), DatahubSchema.getSchema(schema, parameters))
  }


  // TODO: how to set schema
  override def createSource(
      sqlContext: SQLContext,
      metadataPath: String, 
      schema: Option[StructType], 
      providerName: String, 
      parameters: Map[String, String]): Source = {
    new DatahubSource(sqlContext, schema, parameters, metadataPath,
      new DatahubOffsetReader(parameters), getStartOffset(parameters))
  }

  private def getStartOffset(parameters: Map[String, String]): DatahubOffsetRangeLimit = {
    parameters.get("start.offset") match {
      case Some(offset) if offset == "latest" => LatestOffsetRangeLimit
      case Some(offset) if offset == "oldest" => OldestOffsetRangeLimit
      case Some(json) => SpecificOffsetRangeLimit(DatahubSourceOffset.partitionOffsets(json))
      case None => LatestOffsetRangeLimit
    }
  }
}
