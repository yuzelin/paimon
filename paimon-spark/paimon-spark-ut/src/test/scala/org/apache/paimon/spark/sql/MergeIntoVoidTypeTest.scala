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

import org.apache.paimon.spark.PaimonSparkTestBase

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ArrayType, LongType, StringType}

/**
 * Covers `null` / `array()` literals (Spark types them as `void` / `array<void>`) flowing through
 * the merge-schema evolution path. A `void` literal carries no type information: when the column
 * already exists it keeps the target type, when it would be a new column the write fails with a
 * clear message asking for an explicit cast.
 */
class MergeIntoVoidTypeTest extends PaimonSparkTestBase {

  // Schema may be updated during the merge, so the catalog cache must be disabled.
  override protected def sparkConf: SparkConf =
    super.sparkConf.set("spark.sql.catalog.paimon.cache-enabled", "false")

  private def createSourceAndTarget(): Unit = {
    sql("""CREATE TABLE src (a STRING, vessel_info STRUCT<name: STRING>) USING paimon""")
    sql("""INSERT INTO src VALUES ('x', named_struct('name', 'v1'))""")

    // The target already has every column the subquery produces.
    sql("""CREATE TABLE t (
          |  a STRING,
          |  vessel_info STRING,
          |  stops ARRAY<STRING>,
          |  vessel_info2 STRING,
          |  vessel_info_jsonstr STRING
          |) USING paimon""".stripMargin)
  }

  test("Merge into with merge-schema: INSERT * selecting null / array() for existing columns") {
    withTable("src", "t") {
      createSourceAndTarget()

      withSparkSQLConf("spark.paimon.write.merge-schema" -> "true") {
        sql("""MERGE INTO t
              | USING (
              |   SELECT
              |     a,
              |     null AS vessel_info,
              |     array() AS stops,
              |     null AS vessel_info2,
              |     to_json(vessel_info) AS vessel_info_jsonstr
              |   FROM src
              | )
              | ON false
              | WHEN NOT MATCHED THEN INSERT *""".stripMargin)
      }

      // Existing columns keep their type; the schema is unchanged.
      assert(
        spark.table("t").schema.fieldNames.toSeq ==
          Seq("a", "vessel_info", "stops", "vessel_info2", "vessel_info_jsonstr"))
      checkAnswer(
        sql("SELECT a, vessel_info, stops, vessel_info2, vessel_info_jsonstr FROM t"),
        Seq(Row("x", null, Seq.empty[String], null, "{\"name\":\"v1\"}")))
    }
  }

  test("Merge into with merge-schema: explicit cast(null as <type>) keeps working") {
    withTable("src", "t") {
      createSourceAndTarget()

      withSparkSQLConf("spark.paimon.write.merge-schema" -> "true") {
        sql("""MERGE INTO t
              | USING (
              |   SELECT
              |     a,
              |     cast(null AS STRING) AS vessel_info,
              |     cast(array() AS ARRAY<STRING>) AS stops,
              |     cast(null AS STRING) AS vessel_info2,
              |     to_json(vessel_info) AS vessel_info_jsonstr
              |   FROM src
              | )
              | ON false
              | WHEN NOT MATCHED THEN INSERT *""".stripMargin)
      }

      checkAnswer(
        sql("SELECT a, vessel_info, stops, vessel_info2, vessel_info_jsonstr FROM t"),
        Seq(Row("x", null, Seq.empty[String], null, "{\"name\":\"v1\"}")))
    }
  }

  test("Merge into with merge-schema: a null literal does not modify an existing column's type") {
    withTable("t") {
      sql("CREATE TABLE t (a INT, b BIGINT, c ARRAY<STRING>) USING paimon")
      sql("INSERT INTO t VALUES (1, 100L, array('x'))")

      // type-widening is on: a NULL literal still must not widen / alter the existing column type.
      withSparkSQLConf(
        "spark.paimon.write.merge-schema" -> "true",
        "spark.paimon.write.merge-schema.type-widening" -> "true") {
        sql("""MERGE INTO t
              | USING (SELECT 2 AS a, null AS b, array() AS c)
              | ON false
              | WHEN NOT MATCHED THEN INSERT *""".stripMargin)
      }

      val schema = spark.table("t").schema
      assert(schema.fieldNames.toSeq == Seq("a", "b", "c"))
      assert(
        schema("b").dataType == LongType,
        s"b should stay BIGINT but is ${schema("b").dataType}")
      assert(
        schema("c").dataType == ArrayType(StringType),
        s"c should stay ARRAY<STRING> but is ${schema("c").dataType}")
      checkAnswer(
        sql("SELECT a, b, c FROM t ORDER BY a"),
        Seq(Row(1, 100L, Seq("x")), Row(2, null, Seq.empty[String])))
    }
  }

  test("Merge into with merge-schema: INSERT * with a null literal for a NEW column fails clearly") {
    withTable("src", "t") {
      sql("CREATE TABLE src (a STRING) USING paimon")
      sql("INSERT INTO src VALUES ('x')")
      sql("CREATE TABLE t (a STRING) USING paimon")

      withSparkSQLConf("spark.paimon.write.merge-schema" -> "true") {
        val e = intercept[Exception] {
          sql("""MERGE INTO t
                | USING (SELECT a, null AS new_col FROM src)
                | ON false
                | WHEN NOT MATCHED THEN INSERT *""".stripMargin)
        }
        assert(e.getMessage.contains("Cannot infer the type of column"))
      }
    }
  }

  test("Catalog write with merge-schema: INSERT BY NAME null for an existing column") {
    withTable("t") {
      sql("CREATE TABLE t (a INT, b STRING) USING paimon")
      sql("INSERT INTO t VALUES (1, 'x')")

      withSparkSQLConf("spark.paimon.write.merge-schema" -> "true") {
        sql("INSERT INTO t BY NAME SELECT 2 AS a, null AS b")
      }

      checkAnswer(sql("SELECT a, b FROM t ORDER BY a"), Seq(Row(1, "x"), Row(2, null)))
    }
  }

  test("Catalog write with merge-schema: INSERT BY NAME null for a NEW column fails clearly") {
    withTable("t") {
      sql("CREATE TABLE t (a INT) USING paimon")

      withSparkSQLConf("spark.paimon.write.merge-schema" -> "true") {
        val e = intercept[Exception] {
          sql("INSERT INTO t BY NAME SELECT 1 AS a, null AS new_col")
        }
        assert(e.getMessage.contains("Cannot infer the type of column"))
      }
    }
  }
}
