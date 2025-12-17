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

import org.apache.paimon.spark.PaimonMetrics._
import org.apache.paimon.spark.PaimonSparkTestBase

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.junit.jupiter.api.Assertions

abstract class EMRTestBase extends PaimonSparkTestBase {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
//      .set("spark.eventLog.enabled", "true")
//      .set("spark.eventLog.dir", "/Users/zxy/data/spark/history")
  }

  test("pullupAggFilterInMergeScalarSubquery") {
    sql("""
          |CREATE TABLE reason (r_reason_sk INT) using paimon
          |""".stripMargin)
    sql("INSERT INTO reason VALUES (1)")

    sql("""
          |CREATE TABLE store_sales (
          |  ss_quantity INT,
          |  ss_ext_discount_amt DOUBLE,
          |  ss_net_profit DOUBLE
          |) using paimon
          |""".stripMargin)

    sql("""
          |INSERT INTO store_sales VALUES
          |(10, 5.0, 20.0),
          |(30, 8.0, 35.0),
          |(50, 12.0, 50.0),
          |(70, 10.0, 40.0),
          |(90, 15.0, 60.0)
          |""".stripMargin)

    withSparkSQLConf("spark.sql.mergeScalaSubquery.pullupAggFilter" -> "true") {
      checkAnswer(
        sql("""
              |SELECT
              |  CASE
              |    WHEN (
              |      SELECT
              |        count(*)
              |      FROM
              |        store_sales
              |      WHERE
              |        ss_quantity BETWEEN 1
              |        AND 20
              |    ) > 25437 THEN (
              |      SELECT
              |        avg(ss_ext_discount_amt)
              |      FROM
              |        store_sales
              |      WHERE
              |        ss_quantity BETWEEN 1
              |        AND 20
              |    )
              |    ELSE (
              |      SELECT
              |        avg(ss_net_profit)
              |      FROM
              |        store_sales
              |      WHERE
              |        ss_quantity BETWEEN 1
              |        AND 20
              |    )
              |  END bucket1,
              |  CASE
              |    WHEN (
              |      SELECT
              |        count(*)
              |      FROM
              |        store_sales
              |      WHERE
              |        ss_quantity BETWEEN 21
              |        AND 40
              |    ) > 22746 THEN (
              |      SELECT
              |        avg(ss_ext_discount_amt)
              |      FROM
              |        store_sales
              |      WHERE
              |        ss_quantity BETWEEN 21
              |        AND 40
              |    )
              |    ELSE (
              |      SELECT
              |        avg(ss_net_profit)
              |      FROM
              |        store_sales
              |      WHERE
              |        ss_quantity BETWEEN 21
              |        AND 40
              |    )
              |  END bucket2,
              |  CASE
              |    WHEN (
              |      SELECT
              |        count(*)
              |      FROM
              |        store_sales
              |      WHERE
              |        ss_quantity BETWEEN 41
              |        AND 60
              |    ) > 9387 THEN (
              |      SELECT
              |        avg(ss_ext_discount_amt)
              |      FROM
              |        store_sales
              |      WHERE
              |        ss_quantity BETWEEN 41
              |        AND 60
              |    )
              |    ELSE (
              |      SELECT
              |        avg(ss_net_profit)
              |      FROM
              |        store_sales
              |      WHERE
              |        ss_quantity BETWEEN 41
              |        AND 60
              |    )
              |  END bucket3,
              |  CASE
              |    WHEN (
              |      SELECT
              |        count(*)
              |      FROM
              |        store_sales
              |      WHERE
              |        ss_quantity BETWEEN 61
              |        AND 80
              |    ) > 10098 THEN (
              |      SELECT
              |        avg(ss_ext_discount_amt)
              |      FROM
              |        store_sales
              |      WHERE
              |        ss_quantity BETWEEN 61
              |        AND 80
              |    )
              |    ELSE (
              |      SELECT
              |        avg(ss_net_profit)
              |      FROM
              |        store_sales
              |      WHERE
              |        ss_quantity BETWEEN 61
              |        AND 80
              |    )
              |  END bucket4,
              |  CASE
              |    WHEN (
              |      SELECT
              |        count(*)
              |      FROM
              |        store_sales
              |      WHERE
              |        ss_quantity BETWEEN 81
              |        AND 100
              |    ) > 18213 THEN (
              |      SELECT
              |        avg(ss_ext_discount_amt)
              |      FROM
              |        store_sales
              |      WHERE
              |        ss_quantity BETWEEN 81
              |        AND 100
              |    )
              |    ELSE (
              |      SELECT
              |        avg(ss_net_profit)
              |      FROM
              |        store_sales
              |      WHERE
              |        ss_quantity BETWEEN 81
              |        AND 100
              |    )
              |  END bucket5
              |FROM
              |  reason
              |WHERE
              |  r_reason_sk = 1;
              |""".stripMargin),
        Row(20.0, 35.0, 50.0, 40.0, 60.0)
      )
    }
  }

  test(s"Test catalyst filter push down") {
    if (gteqSpark3_4) {
      withSparkSQLConf("spark.paimon.read.pushDownCatalystFilters" -> "true") {
        withTable("t") {
          sql("""
                |CREATE TABLE t (id int, value int, year STRING, month STRING, day STRING, hour STRING)
                |using paimon
                |PARTITIONED BY (year, month, day, hour)
                |""".stripMargin)

          sql("""
                |INSERT INTO t values
                |(1, 100, '2024', '07', '15', '21'),
                |(3, 300, '2025', '07', '16', '22'),
                |(4, 400, '2025', '07', '16', '23'),
                |(5, 440, '2025', '07', '16', '24'),
                |(6, 500, '2025', '07', '17', '00'),
                |(7, 600, '2025', '07', '17', '02')
                |""".stripMargin)

          val q =
            """
              |SELECT * FROM t
              |WHERE CONCAT_WS('-', year, month, day, hour)
              |BETWEEN '2025-07-16-21' AND '2025-07-17-01'
              |ORDER BY id
              |""".stripMargin

          checkAnswer(
            spark.sql(q),
            Seq(
              Row(3, 300, "2025", "07", "16", "22"),
              Row(4, 400, "2025", "07", "16", "23"),
              Row(5, 440, "2025", "07", "16", "24"),
              Row(6, 500, "2025", "07", "17", "00"))
          )

          def checkMetrics(
              s: String,
              scannedSnapshotId: Long,
              skippedTableFiles: Long,
              resultedTableFiles: Long): Unit = {
            val scan = getPaimonScan(s)
            // call getInputPartitions to trigger scan
            scan.inputPartitions
            val metrics = scan.reportDriverMetrics()
            def metric(metrics: Array[CustomTaskMetric], name: String): Long = {
              metrics.find(_.name() == name).get.value()
            }
            Assertions.assertEquals(scannedSnapshotId, metric(metrics, SCANNED_SNAPSHOT_ID))
            Assertions.assertEquals(skippedTableFiles, metric(metrics, SKIPPED_TABLE_FILES))
            Assertions.assertEquals(resultedTableFiles, metric(metrics, RESULTED_TABLE_FILES))
          }

          checkMetrics(q, 1, 2, 4)
        }
      }
    }
  }
}
