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

package org.apache.paimon.spark.emr

import org.apache.paimon.spark.PaimonSparkTestBase

import org.apache.spark.sql.Row

class AffectedRowTest extends PaimonSparkTestBase {

  test("EMR: insert, insert overwrite and dynamic partition overwrite affected row") {
    for (useV2Write <- Seq("true", "false")) {
      withSparkSQLConf("spark.paimon.write.use-v2-write" -> useV2Write) {
        val t = "t"
        withTable(t) {
          sql(s"""
                 |CREATE TABLE $t (id int, pt int)
                 |PARTITIONED BY (pt)
                 |""".stripMargin)

          // INSERT
          checkAnswer(
            sql(s"INSERT INTO $t select id, 1 from range(0, 10)"),
            Seq(Row(10, 0, 0, 10))
          )

          // INSERT OVERWRITE
          checkAnswer(
            sql(s"INSERT OVERWRITE TABLE $t select id, 1 from range(0, 15)"),
            Seq(Row(15, 0, 0, 15))
          )

          // DYNAMIC PARTITION OVERWRITE
          withSQLConf("spark.sql.sources.partitionOverwriteMode" -> "dynamic") {
            checkAnswer(
              sql(s"INSERT OVERWRITE TABLE $t select id, 2 from range(0, 20)"),
              Seq(Row(20, 0, 0, 20))
            )
          }
        }
      }
    }
  }
}
