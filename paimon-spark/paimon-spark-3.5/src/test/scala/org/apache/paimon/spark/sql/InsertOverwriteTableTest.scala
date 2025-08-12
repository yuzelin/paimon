/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row

class InsertOverwriteTableTest extends InsertOverwriteTableTestBase {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
    // .set("spark.eventLog.enabled", "true")
    // .set("spark.eventLog.dir", "/Users/zxy/data/spark/history")
  }

  test("rebalance write single partitions") {
    withSparkSQLConf("spark.paimon.write.use-v2-write" -> "true") {
      withTable("t") {
        sql("""
              |create table t(a int, p1 int) TBLPROPERTIES ('partition.sink-strategy'='rebalance')
              |partitioned by (p1)
              |""".stripMargin)
        sql("insert into t values (1, 1), (2, 2)")
        checkAnswer(sql("select * from t"), Seq(Row(1, 1), Row(2, 2)))
      }
    }
  }

  test("rebalance write multiple partitions") {
    withSparkSQLConf("spark.paimon.write.use-v2-write" -> "true") {
      withTable("t") {
        sql("""
              |create table t (a int, p1 int, p2 int) TBLPROPERTIES ('partition.sink-strategy'='rebalance')
              |partitioned by (p1, p2)
              |""".stripMargin)
        sql("insert into t values (1, 1, 1), (2, 2, 2)")
        checkAnswer(sql("select * from t"), Seq(Row(1, 1, 1), Row(2, 2, 2)))
      }
    }
  }
}
