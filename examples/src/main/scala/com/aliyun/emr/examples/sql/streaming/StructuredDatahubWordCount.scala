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
package com.aliyun.emr.examples.sql.streaming

import org.apache.spark.sql.SparkSession

object StructuredDatahubWordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("datahub-word-count")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._
    val value = spark.readStream.format("datahub")
      .option("endpoint", "https://dh-cn-hangzhou.aliyuncs.com")
      .option("project", "emr_sdk")
      .option("topic", "datahub_streaming_test")
      .option("subscribe.id", "1548156839354DAELZ")
      .option("access.key.id", "LTAI7zv2hihAIKmK")
      .option("access.key.secret", "CdHqmx7UM3d5l3HiU2qUSCF7CCrPn3")
      .option("max.offset.per.trigger", "100")
      .option("zookeeper.connect.address", "47.111.65.177:2181")
      .load()

    val count = value.groupBy("value0", "value1").count()
    val query = count.writeStream.format("console")
      .outputMode("complete")
      .start()
    query.awaitTermination()
  }
}
