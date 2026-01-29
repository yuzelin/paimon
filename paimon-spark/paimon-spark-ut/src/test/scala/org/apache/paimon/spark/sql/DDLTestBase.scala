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

import org.apache.paimon.catalog.Identifier
import org.apache.paimon.schema.Schema
import org.apache.paimon.spark.PaimonSparkTestBase
import org.apache.paimon.types.DataTypes

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.analysis.NoSuchPartitionsException
import org.apache.spark.sql.paimon.shims.memstream.MemoryStream
import org.apache.spark.sql.streaming.StreamingQueryException
import org.apache.spark.sql.types.TimestampType
import org.junit.jupiter.api.Assertions

import java.sql.{Date, Timestamp}
import java.time.LocalDateTime

import scala.collection.JavaConverters._

abstract class DDLTestBase extends PaimonSparkTestBase {

  import testImplicits._

  test("Paimon DDL: create append table with not null") {
    withTable("T") {
      sql("CREATE TABLE T (id INT NOT NULL, name STRING)")

      val e1 = intercept[Exception] {
        sql("""INSERT INTO T VALUES (1, "a"), (2, "b"), (null, "c")""")
      }
      Assertions.assertTrue(e1.getMessage().contains("value appeared in non-nullable field"))

      sql("""INSERT INTO T VALUES (1, "a"), (2, "b"), (3, null)""")
      checkAnswer(
        sql("SELECT * FROM T ORDER BY id"),
        Seq((1, "a"), (2, "b"), (3, null)).toDF()
      )

      val schema = spark.table("T").schema
      Assertions.assertEquals(schema.size, 2)
      Assertions.assertFalse(schema("id").nullable)
      Assertions.assertTrue(schema("name").nullable)
    }
  }
  test("Paimon DDL: create primary-key table with not null") {
    withTable("T") {
      sql("""
            |CREATE TABLE T (id INT, name STRING, pt STRING)
            |TBLPROPERTIES ('primary-key' = 'id,pt')
            |""".stripMargin)

      val e1 = intercept[Exception] {
        sql("""INSERT INTO T VALUES (1, "a", "pt1"), (2, "b", null)""")
      }
      Assertions.assertTrue(e1.getMessage().contains("value appeared in non-nullable field"))

      val e2 = intercept[Exception] {
        sql("""INSERT INTO T VALUES (1, "a", "pt1"), (null, "b", "pt2")""")
      }
      Assertions.assertTrue(e2.getMessage().contains("value appeared in non-nullable field"))

      sql("""INSERT INTO T VALUES (1, "a", "pt1"), (2, "b", "pt1"), (3, null, "pt2")""")
      checkAnswer(
        sql("SELECT * FROM T ORDER BY id"),
        Seq((1, "a", "pt1"), (2, "b", "pt1"), (3, null, "pt2")).toDF()
      )

      val schema = spark.table("T").schema
      Assertions.assertEquals(schema.size, 3)
      Assertions.assertFalse(schema("id").nullable)
      Assertions.assertTrue(schema("name").nullable)
      Assertions.assertFalse(schema("pt").nullable)
    }
  }

  test("Paimon DDL: write nullable expression to non-null column") {
    withTable("T") {
      sql("""
            |CREATE TABLE T (id INT NOT NULL, ts TIMESTAMP NOT NULL)
            |""".stripMargin)

      sql("INSERT INTO T SELECT 1, TO_TIMESTAMP('2024-07-01 16:00:00')")

      checkAnswer(
        sql("SELECT * FROM T ORDER BY id"),
        Row(1, Timestamp.valueOf("2024-07-01 16:00:00")) :: Nil
      )
    }
  }

  test("Paimon DDL: create database with location with filesystem catalog") {
    withTempDir {
      dBLocation =>
        withDatabase("paimon_db") {
          val error = intercept[Exception] {
            spark.sql(s"CREATE DATABASE paimon_db LOCATION '${dBLocation.getCanonicalPath}'")
          }.getMessage
          assert(
            error.contains("Cannot specify location for a database when using fileSystem catalog."))
        }
    }
  }

  test("Paimon DDL: create other table with paimon SparkCatalog") {
    withTable("paimon_tbl1", "paimon_tbl2", "parquet_tbl") {
      spark.sql(s"CREATE TABLE paimon_tbl1 (id int) USING paimon")
      spark.sql(s"CREATE TABLE paimon_tbl2 (id int)")
      val error = intercept[Exception] {
        spark.sql(s"CREATE TABLE parquet_tbl (id int) USING parquet")
      }.getMessage
      assert(error.contains("does not support format table"))
    }
  }

  test("Paimon DDL: create table without using paimon") {
    withTable("paimon_tbl") {
      sql("CREATE TABLE paimon_tbl (id int)")
      assert(!loadTable("paimon_tbl").options().containsKey("provider"))
    }
  }

  test("Paimon DDL: create table like with paimon SparkCatalog") {
    assume(gteqSpark3_4)
    withTable("source_tbl", "target_tbl") {
      sql("""
            |CREATE TABLE source_tbl (
            |  id INT,
            |  name STRING COMMENT 'name column',
            |  pt STRING
            |) COMMENT 'source comment'
            |PARTITIONED BY (pt)
            |TBLPROPERTIES (
            |  'primary-key' = 'id,pt',
            |  'bucket' = '5',
            |  'target-file-size' = '128MB'
            |)
            |""".stripMargin)

      sql("""
            |CREATE TABLE target_tbl
            |LIKE source_tbl
            |TBLPROPERTIES ('bucket' = '8')
            |""".stripMargin)

      val source = loadTable("source_tbl")
      val target = loadTable("target_tbl")

      Assertions.assertEquals(spark.table("source_tbl").schema, spark.table("target_tbl").schema)
      Assertions.assertEquals("source comment", target.comment().get())
      Assertions.assertEquals(List("pt"), target.partitionKeys().asScala.toList)
      Assertions.assertEquals(List("id", "pt"), target.primaryKeys().asScala.toList)
      Assertions.assertEquals("8", target.options().get("bucket"))
      Assertions.assertEquals("128MB", target.options().get("target-file-size"))
      Assertions.assertNotEquals(source.location().toString, target.location().toString)
    }
  }

  test("Paimon DDL: create table like from branch with paimon SparkCatalog") {
    assume(gteqSpark3_4)
    withTable("source_tbl", "target_tbl") {
      sql("""
            |CREATE TABLE source_tbl (
            |  id INT,
            |  name STRING COMMENT 'name column',
            |  pt STRING
            |) COMMENT 'source comment'
            |PARTITIONED BY (pt)
            |TBLPROPERTIES (
            |  'primary-key' = 'id,pt',
            |  'bucket' = '5'
            |)
            |""".stripMargin)
      sql("INSERT INTO source_tbl VALUES (1, 'a', 'p1')")

      checkAnswer(
        sql("CALL paimon.sys.create_branch(table => 'test.source_tbl', branch => 'test_branch')"),
        Row(true) :: Nil)
      sql("ALTER TABLE `source_tbl$branch_test_branch` ADD COLUMNS(extra STRING)")

      sql("""
            |CREATE TABLE target_tbl
            |LIKE `source_tbl$branch_test_branch`
            |""".stripMargin)

      val target = loadTable("target_tbl")

      Assertions.assertFalse(spark.table("source_tbl").schema.fieldNames.contains("extra"))
      Assertions.assertTrue(spark.table("target_tbl").schema.fieldNames.contains("extra"))
      Assertions.assertEquals(
        sql("SELECT * FROM `source_tbl$branch_test_branch`").schema,
        spark.table("target_tbl").schema)
      Assertions.assertEquals("source comment", target.comment().get())
      Assertions.assertEquals(List("pt"), target.partitionKeys().asScala.toList)
      Assertions.assertEquals(List("id", "pt"), target.primaryKeys().asScala.toList)
      Assertions.assertEquals("5", target.options().get("bucket"))
    }
  }

  test("Paimon DDL: create table like if not exists with paimon SparkCatalog") {
    assume(gteqSpark3_4)
    withTable("source_tbl", "target_tbl") {
      sql("""
            |CREATE TABLE source_tbl (
            |  id INT,
            |  name STRING,
            |  pt STRING
            |)
            |PARTITIONED BY (pt)
            |TBLPROPERTIES (
            |  'primary-key' = 'id,pt',
            |  'bucket' = '5'
            |)
            |""".stripMargin)

      sql("""
            |CREATE TABLE target_tbl (
            |  id BIGINT,
            |  pt STRING
            |) COMMENT 'target comment'
            |PARTITIONED BY (pt)
            |TBLPROPERTIES (
            |  'primary-key' = 'id,pt',
            |  'bucket' = '3'
            |)
            |""".stripMargin)

      val targetSchema = spark.table("target_tbl").schema
      val targetLocation = loadTable("target_tbl").location().toString

      sql("""
            |CREATE TABLE IF NOT EXISTS target_tbl
            |LIKE source_tbl
            |""".stripMargin)

      val target = loadTable("target_tbl")

      Assertions.assertEquals(targetSchema, spark.table("target_tbl").schema)
      Assertions.assertFalse(spark.table("target_tbl").schema.fieldNames.contains("name"))
      Assertions.assertEquals("target comment", target.comment().get())
      Assertions.assertEquals("3", target.options().get("bucket"))
      Assertions.assertEquals(targetLocation, target.location().toString)
    }
  }

  test("Paimon DDL: create table like stored as is unsupported with paimon SparkCatalog") {
    assume(gteqSpark3_4)
    withTable("source_tbl", "target_tbl") {
      sql("CREATE TABLE source_tbl (id INT)")

      val error = intercept[Exception] {
        sql("""
              |CREATE TABLE target_tbl
              |LIKE source_tbl
              |STORED AS PARQUET
              |""".stripMargin)
      }.getMessage

      Assertions.assertTrue(
        error.contains("CREATE TABLE LIKE ... STORED AS is not supported for SparkCatalog."))
    }
  }

  test("Paimon DDL: REPLACE TABLE replaces in-place and preserves old snapshots") {
    assume(gteqSpark3_4)
    withTable("t") {
      sql("""
            |CREATE TABLE t (id BIGINT, data STRING)
            |USING paimon
            |TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '2')
            |""".stripMargin)
      sql("INSERT INTO t VALUES (1, 'old')")
      val oldLocation = loadTable("t").location().toString
      val oldSnapshotId = loadTable("t").snapshotManager().latestSnapshotId()

      sql("""
            |REPLACE TABLE t (id BIGINT, name STRING)
            |USING paimon
            |TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '4')
            |""".stripMargin)

      val replaced = loadTable("t")
      Assertions.assertEquals(oldLocation, replaced.location().toString)
      Assertions.assertEquals("4", replaced.options().get("bucket"))
      Assertions.assertEquals(Seq("id", "name"), spark.table("t").schema.fieldNames.toSeq)
      checkAnswer(sql("SELECT * FROM t"), Seq.empty[Row])

      checkAnswer(
        sql(s"SELECT id, data FROM t VERSION AS OF $oldSnapshotId"),
        Seq((1L, "old")).toDF())
    }
  }

  test("Paimon DDL: REPLACE TABLE without SELECT fails if table is missing") {
    assume(gteqSpark3_4)
    withTable("missing") {
      val error = intercept[AnalysisException] {
        sql("""
              |REPLACE TABLE missing (id BIGINT, data STRING)
              |USING paimon
              |TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '2')
              |""".stripMargin)
      }.getMessage

      Assertions.assertTrue(
        error.contains("TABLE_OR_VIEW_NOT_FOUND") ||
          error.contains("cannot be found") ||
          error.contains("not found"))
    }
  }

  test("Paimon DDL: CREATE TABLE fails when table exists") {
    withTable("t") {
      sql("CREATE TABLE t (id BIGINT, data STRING) USING paimon")

      val error = intercept[AnalysisException] {
        sql("CREATE TABLE t (id BIGINT, name STRING) USING paimon")
      }.getMessage

      Assertions.assertTrue(
        error.contains("TABLE_OR_VIEW_ALREADY_EXISTS") || error.contains("already exists"))
    }
  }

  test("Paimon DDL: CREATE OR REPLACE TABLE AS SELECT on partitioned table") {
    assume(gteqSpark3_4)
    withTable("t") {
      withTempView("source") {
        sql("""
              |CREATE TABLE t (id BIGINT, data STRING, pt STRING)
              |USING paimon
              |PARTITIONED BY (pt)
              |TBLPROPERTIES ('primary-key' = 'id,pt', 'bucket' = '2')
              |""".stripMargin)
        sql("INSERT INTO t VALUES (1, 'old', 'p0')")
        val oldLocation = loadTable("t").location().toString
        Seq((2L, "x2", "p1"), (3L, "x3", "p2"))
          .toDF("id", "data", "pt")
          .createOrReplaceTempView("source")

        sql("""
              |CREATE OR REPLACE TABLE t
              |USING paimon
              |PARTITIONED BY (pt)
              |TBLPROPERTIES ('primary-key' = 'id,pt', 'bucket' = '3')
              |AS SELECT * FROM source
              |""".stripMargin)

        val replaced = loadTable("t")
        Assertions.assertEquals(oldLocation, replaced.location().toString)
        Assertions.assertEquals("3", replaced.options().get("bucket"))
        checkAnswer(
          sql("SELECT * FROM t ORDER BY id"),
          Seq((2L, "x2", "p1"), (3L, "x3", "p2")).toDF())
      }
    }
  }

  test("Paimon DDL: CREATE OR REPLACE TABLE AS SELECT supports incompatible schema") {
    assume(gteqSpark3_4)
    withTable("t") {
      withTempView("source") {
        sql("""
              |CREATE TABLE t (id BIGINT, data STRING)
              |USING paimon
              |TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '2')
              |""".stripMargin)
        sql("INSERT INTO t VALUES (1, 'old')")
        val oldLocation = loadTable("t").location().toString
        val oldSnapshotId = loadTable("t").snapshotManager().latestSnapshotId()
        Seq(("2", 20), ("3", 30)).toDF("id", "amount").createOrReplaceTempView("source")

        sql("""
              |CREATE OR REPLACE TABLE t
              |USING paimon
              |TBLPROPERTIES ('bucket' = '-1')
              |AS SELECT * FROM source
              |""".stripMargin)

        val replaced = loadTable("t")
        Assertions.assertEquals(oldLocation, replaced.location().toString)
        Assertions.assertEquals("-1", replaced.options().get("bucket"))
        Assertions.assertEquals(Seq("id", "amount"), spark.table("t").schema.fieldNames.toSeq)
        Assertions.assertEquals("string", spark.table("t").schema("id").dataType.typeName)
        Assertions.assertEquals("integer", spark.table("t").schema("amount").dataType.typeName)
        checkAnswer(sql("SELECT * FROM t ORDER BY id"), Seq(("2", 20), ("3", 30)).toDF())
        checkAnswer(
          sql(s"SELECT id, data FROM t VERSION AS OF $oldSnapshotId"),
          Seq((1L, "old")).toDF())
      }
    }
  }

  test(
    "Paimon DDL: CREATE OR REPLACE TABLE AS SELECT reads latest rows after incompatible nested type replace") {
    assume(gteqSpark3_4)
    withTable("src", "t") {
      sql("""
            |CREATE TABLE src (
            |  id BIGINT,
            |  payload DOUBLE,
            |  name_a STRING,
            |  name_b STRING
            |)
            |USING paimon
            |TBLPROPERTIES ('bucket' = '-1')
            |""".stripMargin)
      sql("""
            |INSERT INTO src VALUES
            |  (1, 1.1D, 'a', 'x'),
            |  (2, 2.2D, 'b', 'y')
            |""".stripMargin)

      sql("""
            |CREATE TABLE t
            |USING paimon
            |TBLPROPERTIES ('bucket' = '-1')
            |AS SELECT * FROM src
            |""".stripMargin)

      sql("""
            |CREATE OR REPLACE TABLE t
            |USING paimon
            |TBLPROPERTIES ('bucket' = '-1')
            |AS
            |SELECT
            |  id,
            |  named_struct(
            |    'items_before', array(name_a),
            |    'items_after', array(name_b)
            |  ) AS payload,
            |  name_a,
            |  name_b
            |FROM src
            |""".stripMargin)

      Assertions.assertEquals("struct", spark.table("t").schema("payload").dataType.typeName)

      checkAnswer(
        sql("""
              |SELECT id, payload.items_before, payload.items_after, name_a, name_b
              |FROM t
              |WHERE name_a = 'a'
              |LIMIT 1
              |""".stripMargin),
        Row(1L, Seq("a"), Seq("x"), "a", "x") :: Nil
      )
    }
  }

  test("Paimon DDL: REPLACE TABLE supports incompatible schema and preserves old snapshots") {
    assume(gteqSpark3_4)
    withTable("t") {
      sql("""
            |CREATE TABLE t (id BIGINT, data STRING)
            |USING paimon
            |TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '2')
            |""".stripMargin)
      sql("INSERT INTO t VALUES (1, 'old')")
      val oldLocation = loadTable("t").location().toString
      val oldSnapshotId = loadTable("t").snapshotManager().latestSnapshotId()

      sql("""
            |REPLACE TABLE t (id STRING, amount INT)
            |USING paimon
            |TBLPROPERTIES ('bucket' = '-1')
            |""".stripMargin)

      val replaced = loadTable("t")
      Assertions.assertEquals(oldLocation, replaced.location().toString)
      Assertions.assertEquals("-1", replaced.options().get("bucket"))
      Assertions.assertEquals(Seq("id", "amount"), spark.table("t").schema.fieldNames.toSeq)
      Assertions.assertEquals("string", spark.table("t").schema("id").dataType.typeName)
      Assertions.assertEquals("integer", spark.table("t").schema("amount").dataType.typeName)
      checkAnswer(sql("SELECT * FROM t"), Seq.empty[Row])
      checkAnswer(
        sql(s"SELECT id, data FROM t VERSION AS OF $oldSnapshotId"),
        Seq((1L, "old")).toDF())
    }
  }

  test("Paimon DDL: REPLACE TABLE AS SELECT from same table preserves data") {
    assume(gteqSpark3_4)
    withTable("t") {
      sql("""
            |CREATE TABLE t (id INT, data STRING)
            |USING paimon
            |TBLPROPERTIES ('bucket' = '-1')
            |""".stripMargin)
      sql("INSERT INTO t VALUES (1, 'a'), (2, 'b')")

      // Self-referencing RTAS: should read old data, not the truncated data
      sql("CREATE OR REPLACE TABLE t TBLPROPERTIES ('bucket' = '-1') AS SELECT * FROM t")
      checkAnswer(sql("SELECT * FROM t ORDER BY id"), Row(1, "a") :: Row(2, "b") :: Nil)
    }
  }

  test("Paimon DDL: REPLACE TABLE AS SELECT with time travel reads specified snapshot") {
    assume(gteqSpark3_4)
    withTable("t") {
      sql("""
            |CREATE TABLE t (id INT, data STRING)
            |USING paimon
            |TBLPROPERTIES ('bucket' = '-1')
            |""".stripMargin)
      sql("INSERT INTO t VALUES (1, 'v1')")
      val snapshotId1 = loadTable("t").snapshotManager().latestSnapshotId()
      sql("INSERT INTO t VALUES (2, 'v2')")

      // RTAS with VERSION AS OF should read the specified snapshot, not the latest
      sql(
        s"CREATE OR REPLACE TABLE t TBLPROPERTIES ('bucket' = '-1') AS SELECT * FROM t VERSION AS OF $snapshotId1")
      checkAnswer(sql("SELECT * FROM t ORDER BY id"), Row(1, "v1") :: Nil)
    }
  }

  fileFormats.foreach {
    format =>
      test(s"Paimon DDL: create table with char/varchar/string, file.format: $format") {
        withTable("paimon_tbl") {
          spark.sql(
            s"""
               |CREATE TABLE paimon_tbl (id int, col_s1 char(9), col_s2 varchar(10), col_s3 string)
               |USING PAIMON
               |TBLPROPERTIES ('file.format' = '$format')
               |""".stripMargin)

          spark.sql(s"""
                       |insert into paimon_tbl values
                       |(1, 'Wednesday', 'Wednesday', 'Wednesday'),
                       |(2, 'Friday', 'Friday', 'Friday')
                       |""".stripMargin)

          // check description
          checkAnswer(
            spark
              .sql(s"DESC paimon_tbl")
              .select("col_name", "data_type")
              .where("col_name LIKE 'col_%'")
              .orderBy("col_name"),
            Row("col_s1", "char(9)") :: Row("col_s2", "varchar(10)") :: Row(
              "col_s3",
              "string") :: Nil
          )

          // check select
          if (format == "orc" && !gteqSpark3_4) {
            // Orc reader will right trim the char type, e.g. "Friday   " => "Friday" (see orc's `CharTreeReader`)
            // and Spark has a conf `spark.sql.readSideCharPadding` to auto padding char only since 3.4 (default true)
            // So when using orc with Spark3.4-, here will return "Friday"
            checkAnswer(
              spark.sql(s"select col_s1 from paimon_tbl where id = 2"),
              Row("Friday") :: Nil
            )
            // Spark will auto create the filter like Filter(isnotnull(col_s1#124) AND (col_s1#124 = Friday   ))
            // for char type, so here will not return any rows
            checkAnswer(
              spark.sql(s"select col_s1 from paimon_tbl where col_s1 = 'Friday'"),
              Nil
            )
          } else {
            checkAnswer(
              spark.sql(s"select col_s1 from paimon_tbl where id = 2"),
              Row("Friday   ") :: Nil
            )
            checkAnswer(
              spark.sql(s"select col_s1 from paimon_tbl where col_s1 = 'Friday'"),
              Row("Friday   ") :: Nil
            )
          }
          checkAnswer(
            spark.sql(s"select col_s2 from paimon_tbl where col_s2 = 'Friday'"),
            Row("Friday") :: Nil
          )
          checkAnswer(
            spark.sql(s"select col_s3 from paimon_tbl where col_s3 = 'Friday'"),
            Row("Friday") :: Nil
          )
        }
      }
  }

  test("Paimon DDL: write with char") {
    withTable("paimon_tbl") {
      spark.sql(s"""
                   |CREATE TABLE paimon_tbl (id int, c char(6))
                   |USING PAIMON
                   |""".stripMargin)

      withSparkSQLConf("spark.sql.legacy.charVarcharAsString" -> "true") {
        sql("INSERT INTO paimon_tbl VALUES (1, 'ab')")
      }

      withSparkSQLConf("spark.sql.legacy.charVarcharAsString" -> "false") {
        sql("INSERT INTO paimon_tbl VALUES (2, 'ab')")
      }

      if (gteqSpark3_4) {
        withSparkSQLConf("spark.sql.readSideCharPadding" -> "true") {
          checkAnswer(
            spark.sql("SELECT c FROM paimon_tbl ORDER BY id"),
            Row("ab    ") :: Row("ab    ") :: Nil)
        }
        withSparkSQLConf("spark.sql.readSideCharPadding" -> "false") {
          checkAnswer(
            spark.sql("SELECT c FROM paimon_tbl ORDER BY id"),
            Row("ab") :: Row("ab    ") :: Nil)
        }
      } else {
        checkAnswer(
          spark.sql("SELECT c FROM paimon_tbl ORDER BY id"),
          Row("ab") :: Row("ab    ") :: Nil)
      }
    }
  }

  test("Paimon DDL: create table with timestamp/timestamp_ntz") {
    Seq("orc", "parquet", "avro").foreach {
      format =>
        Seq(true, false).foreach {
          datetimeJava8APIEnabled =>
            withSparkSQLConf(
              "spark.sql.datetime.java8API.enabled" -> datetimeJava8APIEnabled.toString) {
              withTimeZone("Asia/Shanghai") {
                withTable("paimon_tbl") {
                  // Spark support create table with timestamp_ntz since 3.4
                  if (gteqSpark3_4) {
                    sql(s"""
                           |CREATE TABLE paimon_tbl (id int, binary BINARY, ts timestamp, ts_ntz timestamp_ntz)
                           |USING paimon
                           |TBLPROPERTIES ('file.format'='$format')
                           |""".stripMargin)

                    sql(s"INSERT INTO paimon_tbl VALUES (1, binary('b'), timestamp'2024-01-01 00:00:00', timestamp_ntz'2024-01-01 00:00:00')")
                    checkAnswer(
                      sql(s"SELECT ts, ts_ntz FROM paimon_tbl"),
                      Row(
                        if (datetimeJava8APIEnabled)
                          Timestamp.valueOf("2024-01-01 00:00:00").toInstant
                        else Timestamp.valueOf("2024-01-01 00:00:00"),
                        LocalDateTime.parse("2024-01-01T00:00:00")
                      )
                    )

                    // change time zone to UTC
                    withTimeZone("UTC") {
                      // todo: fix with orc
                      if (format != "orc")
                        checkAnswer(
                          sql(s"SELECT ts, ts_ntz FROM paimon_tbl"),
                          Row(
                            if (datetimeJava8APIEnabled)
                              Timestamp.valueOf("2023-12-31 16:00:00").toInstant
                            else Timestamp.valueOf("2023-12-31 16:00:00"),
                            LocalDateTime.parse("2024-01-01T00:00:00")
                          )
                        )
                    }
                  } else {
                    sql(s"""
                           |CREATE TABLE paimon_tbl (id int, binary BINARY, ts timestamp)
                           |USING paimon
                           |TBLPROPERTIES ('file.format'='$format')
                           |""".stripMargin)

                    sql(s"INSERT INTO paimon_tbl VALUES (1, binary('b'), timestamp'2024-01-01 00:00:00')")
                    checkAnswer(
                      sql(s"SELECT ts FROM paimon_tbl"),
                      Row(
                        if (datetimeJava8APIEnabled)
                          Timestamp.valueOf("2024-01-01 00:00:00").toInstant
                        else Timestamp.valueOf("2024-01-01 00:00:00"))
                    )

                    // For Spark 3.3 and below, time zone conversion is not supported,
                    // see TypeUtils.treatPaimonTimestampTypeAsSparkTimestampType
                    withTimeZone("UTC") {
                      // todo: fix with orc
                      if (format != "orc") {
                        checkAnswer(
                          sql(s"SELECT ts FROM paimon_tbl"),
                          Row(
                            if (datetimeJava8APIEnabled)
                              Timestamp.valueOf("2024-01-01 00:00:00").toInstant
                            else Timestamp.valueOf("2024-01-01 00:00:00"))
                        )
                      }
                    }
                  }
                }
              }
            }
        }
    }
  }

  test("Paimon DDL: legacy timestamp mapping") {
    assume(gteqSpark3_4)

    Seq("orc", "parquet").foreach {
      format =>
        withSparkSQLConf("spark.paimon.legacy-timestamp-mapping.enabled" -> "true") {
          withTimeZone("Asia/Shanghai") {
            withTable("paimon_tbl") {
              sql(s"""
                     |CREATE TABLE paimon_tbl (reported_time timestamp)
                     |USING paimon
                     |TBLPROPERTIES ('file.format'='$format')
                     |""".stripMargin)

              sql("INSERT INTO paimon_tbl VALUES (timestamp'2026-06-30 23:47:51')")

              Assertions.assertEquals(
                TimestampType,
                spark.table("paimon_tbl").schema("reported_time").dataType)
              checkAnswer(
                sql("""
                      |SELECT from_unixtime(
                      |  unix_timestamp(reported_time) + 24 * 3600,
                      |  'yyyy-MM-dd HH:mm:ss'
                      |) FROM paimon_tbl
                      |""".stripMargin),
                Row("2026-07-01 23:47:51") :: Nil
              )
            }
          }
        }
    }
  }

  test("Paimon DDL: create table with timestamp/timestamp_ntz using table API") {
    val identifier = Identifier.create("test", "paimon_tbl")
    try {
      withTimeZone("Asia/Shanghai") {
        val schema = Schema.newBuilder
          .column("ts", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE())
          .column("ts_ntz", DataTypes.TIMESTAMP())
          .build
        paimonCatalog.createTable(identifier, schema, false)
        sql(
          s"INSERT INTO paimon_tbl VALUES (timestamp'2024-01-01 00:00:00', timestamp_ntz'2024-01-01 00:00:00')")

        // read by spark
        checkAnswer(
          sql(s"SELECT ts, ts_ntz FROM paimon_tbl"),
          Row(
            Timestamp.valueOf("2024-01-01 00:00:00"),
            if (gteqSpark3_4) LocalDateTime.parse("2024-01-01T00:00:00")
            else Timestamp.valueOf("2024-01-01 00:00:00")
          )
        )

        // read by table api
        // Due to previous design, read timestamp ltz type with spark 3.3 and below will cause problems,
        // skip testing it
        if (gteqSpark3_4) {
          val table = paimonCatalog.getTable(identifier)
          val builder = table.newReadBuilder.withProjection(Array[Int](0, 1))
          val splits = builder.newScan().plan().splits()
          builder.newRead
            .createReader(splits)
            .forEachRemaining(
              r => {
                Assertions.assertEquals(
                  Timestamp.valueOf("2023-12-31 16:00:00"),
                  r.getTimestamp(0, 6).toSQLTimestamp)
                Assertions.assertEquals(
                  Timestamp.valueOf("2024-01-01 00:00:00").toLocalDateTime,
                  r.getTimestamp(1, 6).toLocalDateTime)
              })
        }

        // change time zone to UTC
        withTimeZone("UTC") {
          // read by spark
          checkAnswer(
            sql(s"SELECT ts, ts_ntz FROM paimon_tbl"),
            Row(
              // For Spark 3.3 and below, time zone conversion is not supported,
              // see TypeUtils.treatPaimonTimestampTypeAsSparkTimestampType
              if (gteqSpark3_4) Timestamp.valueOf("2023-12-31 16:00:00")
              else Timestamp.valueOf("2024-01-01 00:00:00"),
              if (gteqSpark3_4) LocalDateTime.parse("2024-01-01T00:00:00")
              else Timestamp.valueOf("2024-01-01 00:00:00")
            )
          )

          // read by table api
          // Due to previous design, read timestamp ltz type with spark 3.3 and below will cause problems,
          // skip testing it
          if (gteqSpark3_4) {
            val table = paimonCatalog.getTable(identifier)
            val builder = table.newReadBuilder.withProjection(Array[Int](0, 1))
            val splits = builder.newScan().plan().splits()
            builder.newRead
              .createReader(splits)
              .forEachRemaining(
                r => {
                  Assertions.assertEquals(
                    Timestamp.valueOf("2023-12-31 16:00:00"),
                    r.getTimestamp(0, 6).toSQLTimestamp)
                  Assertions.assertEquals(
                    Timestamp.valueOf("2024-01-01 00:00:00").toLocalDateTime,
                    r.getTimestamp(1, 6).toLocalDateTime)
                })
          }
        }
      }
    } finally {
      paimonCatalog.dropTable(identifier, true)
    }
  }

  test("Paimon DDL: select table with timestamp and timestamp_ntz with filter") {
    Seq(true, false).foreach {
      datetimeJava8APIEnabled =>
        withSparkSQLConf("spark.sql.datetime.java8API.enabled" -> datetimeJava8APIEnabled.toString) {
          withTable("paimon_tbl") {
            // Spark support create table with timestamp_ntz since 3.4
            if (gteqSpark3_4) {
              sql(s"""
                     |CREATE TABLE paimon_tbl (ts timestamp, ts_ntz timestamp_ntz)
                     |USING paimon
                     |""".stripMargin)
              sql(
                s"INSERT INTO paimon_tbl VALUES (timestamp'2024-01-01 00:00:00', timestamp_ntz'2024-01-01 00:00:00')")
              sql(
                s"INSERT INTO paimon_tbl VALUES (timestamp'2024-01-02 00:00:00', timestamp_ntz'2024-01-02 00:00:00')")
              sql(
                s"INSERT INTO paimon_tbl VALUES (timestamp'2024-01-03 00:00:00', timestamp_ntz'2024-01-03 00:00:00')")

              checkAnswer(
                sql(s"SELECT * FROM paimon_tbl where ts_ntz = timestamp_ntz'2024-01-01 00:00:00'"),
                Row(
                  if (datetimeJava8APIEnabled)
                    Timestamp.valueOf("2024-01-01 00:00:00").toInstant
                  else Timestamp.valueOf("2024-01-01 00:00:00"),
                  LocalDateTime.parse("2024-01-01T00:00:00")
                )
              )

              checkAnswer(
                sql(s"SELECT * FROM paimon_tbl where ts > timestamp'2024-01-02 00:00:00'"),
                Row(
                  if (datetimeJava8APIEnabled)
                    Timestamp.valueOf("2024-01-03 00:00:00").toInstant
                  else Timestamp.valueOf("2024-01-03 00:00:00"),
                  LocalDateTime.parse("2024-01-03T00:00:00")
                )
              )
            } else {
              sql(s"""
                     |CREATE TABLE paimon_tbl (ts timestamp)
                     |USING paimon
                     |""".stripMargin)
              sql(s"INSERT INTO paimon_tbl VALUES (timestamp'2024-01-01 00:00:00')")
              sql(s"INSERT INTO paimon_tbl VALUES (timestamp'2024-01-02 00:00:00')")
              sql(s"INSERT INTO paimon_tbl VALUES (timestamp'2024-01-03 00:00:00')")

              checkAnswer(
                sql(s"SELECT * FROM paimon_tbl where ts = timestamp'2024-01-01 00:00:00'"),
                Row(
                  if (datetimeJava8APIEnabled)
                    Timestamp.valueOf("2024-01-01 00:00:00").toInstant
                  else Timestamp.valueOf("2024-01-01 00:00:00"))
              )
            }
          }
        }
    }
  }

  test("Paimon DDL: create table with unsupported partitioned by") {
    val error = intercept[RuntimeException] {
      sql(s"""
             |CREATE TABLE T (id STRING, name STRING, pt STRING)
             |PARTITIONED BY (substr(pt, 1, 2))
             |""".stripMargin)
    }.getMessage
    assert(error.contains("Unsupported partition transform"))
  }

  test("Fix partition column generate wrong partition spec") {
    Seq(true, false).foreach {
      legacyPartName =>
        withTable("p_t") {
          spark.sql(s"""
                       |CREATE TABLE p_t (
                       |    id BIGINT,
                       |    c1 STRING
                       |) using paimon
                       |PARTITIONED BY (day binary)
                       |tblproperties('partition.legacy-name'='$legacyPartName');
                       |""".stripMargin)

          if (legacyPartName) {
            spark.sql("insert into table p_t values(1, 'a', cast('2021' as binary))")
            intercept[Exception] {
              spark.sql("SELECT * FROM p_t").collect()
            }
          } else {
            spark.sql("insert into table p_t values(1, 'a', cast('2021' as binary))")
            checkAnswer(spark.sql("SELECT * FROM p_t"), Row(1, "a", "2021".getBytes))
            val path = spark.sql("SELECT __paimon_file_path FROM p_t").collect()
            assert(path.length == 1)
            assert(path.head.getString(0).contains("/day=2021/"))
          }
        }

        withTable("p_t") {
          spark.sql(s"""
                       |CREATE TABLE p_t (
                       |    id BIGINT,
                       |    c1 STRING
                       |) using paimon
                       |PARTITIONED BY (day date)
                       |tblproperties('partition.legacy-name'='$legacyPartName');
                       |""".stripMargin)

          spark.sql("insert into table p_t values(1, 'a', cast('2021-01-01' as date))")
          checkAnswer(spark.sql("SELECT * FROM p_t"), Row(1, "a", Date.valueOf("2021-01-01")))

          val path = spark.sql("SELECT __paimon_file_path FROM p_t").collect()
          assert(path.length == 1)
          if (legacyPartName) {
            assert(path.head.getString(0).contains("/day=18628/"))
          } else {
            assert(path.head.getString(0).contains("/day=2021-01-01/"))
          }
        }
    }
  }

  test("Paimon DDL: create and drop external / managed table") {
    withTempDir {
      tbLocation =>
        withTable("external_tbl", "managed_tbl") {
          // create external table
          val error = intercept[UnsupportedOperationException] {
            sql(
              s"CREATE TABLE external_tbl (id INT) USING paimon LOCATION '${tbLocation.getCanonicalPath}'")
          }.getMessage
          assert(error.contains("not support"))

          // create managed table
          sql("CREATE TABLE managed_tbl (id INT) USING paimon")
          val table = loadTable("managed_tbl")
          val fileIO = table.fileIO()
          val tableLocation = table.location()

          // drop managed table
          sql("DROP TABLE managed_tbl")
          assert(!fileIO.exists(tableLocation))
        }
    }
  }

  test("Paimon DDL: rename table with catalog name") {
    sql("USE default")
    withTable("t1", "t2") {
      sql("CREATE TABLE t1 (id INT) USING paimon")
      sql("INSERT INTO t1 VALUES 1")
      sql("ALTER TABLE paimon.default.t1 RENAME TO paimon.default.t2")
      checkAnswer(sql("SELECT * FROM t2"), Row(1))

      assert(intercept[Exception] {
        sql("ALTER TABLE paimon.default.t2 RENAME TO spark_catalog.default.t2")
      }.getMessage.contains("Only supports operations within the same catalog"))
    }
  }

  test("Paimon DDL: create unsupported table") {
    assert(intercept[Exception] {
      sql("CREATE TABLE t (id INT) USING paimon1")
    }.getMessage.contains("Provider 'paimon1' is not supported"))
  }

  test("Paimon DDL: Drop Partition by partial spec") {
    withTable("tbl") {
      spark.sql(
        s"CREATE TABLE tbl (id int, data string) USING paimon " +
          s"PARTITIONED BY (dt string, hour string, event string) ")
      spark.sql(s"INSERT INTO tbl VALUES (1, 'a', '2023-01-01', '00', 'event1')")
      spark.sql(s"INSERT INTO tbl VALUES (1, 'a', '2023-01-02', '00', 'event1')")
      spark.sql(s"INSERT INTO tbl VALUES (1, 'a', '2023-01-02', '00', 'event2')")
      spark.sql(s"INSERT INTO tbl VALUES (1, 'a', '2023-01-02', '00', 'event3')")
      spark.sql(s"INSERT INTO tbl VALUES (1, 'a', '2023-01-02', '02', 'event1')")
      spark.sql(s"INSERT INTO tbl VALUES (1, 'a', '2023-01-02', '02', 'event2')")
      spark.sql(s"INSERT INTO tbl VALUES (1, 'a', '2023-01-02', '03', 'event1')")
      spark.sql(s"INSERT INTO tbl VALUES (1, 'a', '2023-01-03', '00', 'event1')")
      val query = () => spark.sql("SELECT * FROM tbl")
      assert(query().count() == 8)
      // drop full parts level
      spark.sql("ALTER TABLE tbl DROP PARTITION (dt='2023-01-01', hour='00', event='event1')")
      assert(query().count() == 7)
      // drop first + second level
      spark.sql("ALTER TABLE tbl DROP PARTITION (dt='2023-01-02', hour='00')")
      assert(query().count() == 4)
      // drop first level
      spark.sql("ALTER TABLE tbl DROP PARTITION (dt='2023-01-02')")
      assert(query().count() == 1)
      // no effected drop
      spark.sql("ALTER TABLE tbl DROP PARTITION (dt='2023-01-01')")
      assert(query().count() == 1)
      assertThrows[AnalysisException] {
        spark.sql("ALTER TABLE tbl DROP PARTITION (hour='00', event='event1')")
      }
      assertThrows[NoSuchPartitionsException] {
        spark.sql("ALTER TABLE tbl DROP PARTITION (dt='2023-01-01', hour='00', event='event1')")
      }
    }
  }

  test("Paimon DDL: alter column SET/DROP NOT NULL") {
    withTable("T") {
      // Create table with NOT NULL constraint
      sql("CREATE TABLE T (id INT NOT NULL, name STRING NOT NULL, age INT)")
      sql("""INSERT INTO T VALUES (1, "a", 10), (2, "b", 20)""")

      // Verify initial schema
      var schema = spark.table("T").schema
      Assertions.assertFalse(schema("id").nullable)
      Assertions.assertFalse(schema("name").nullable)
      Assertions.assertTrue(schema("age").nullable)

      // DROP NOT NULL on 'name' column
      sql("ALTER TABLE T ALTER COLUMN name DROP NOT NULL")
      schema = spark.table("T").schema
      Assertions.assertFalse(schema("id").nullable)
      Assertions.assertTrue(schema("name").nullable) // Should be nullable now
      Assertions.assertTrue(schema("age").nullable)

      // Now we can insert null for 'name'
      sql("""INSERT INTO T VALUES (3, null, 30)""")
      checkAnswer(
        sql("SELECT * FROM T ORDER BY id"),
        Seq(Row(1, "a", 10), Row(2, "b", 20), Row(3, null, 30))
      )

      // SET NOT NULL on 'age' column (our custom rule bypasses Spark's restriction)
      sql("ALTER TABLE T ALTER COLUMN age SET NOT NULL")
      schema = spark.table("T").schema
      Assertions.assertFalse(schema("id").nullable)
      Assertions.assertTrue(schema("name").nullable)
      Assertions.assertFalse(schema("age").nullable) // Should be non-nullable now

      // Verify that null values can no longer be inserted for 'age'
      val e = intercept[Exception] {
        sql("""INSERT INTO T VALUES (4, "d", null)""")
      }
      Assertions.assertTrue(e.getMessage().contains("value appeared in non-nullable field"))
    }
  }

  test("Paimon DDL: SET NOT NULL validates existing data") {
    withTable("T") {
      sql("CREATE TABLE T (id INT, value INT) USING paimon")

      // Insert data including NULL values
      sql("INSERT INTO T VALUES (1, 100)")
      sql("INSERT INTO T VALUES (2, null)")
      sql("INSERT INTO T VALUES (3, 200)")

      // SET NOT NULL should fail because existing data has NULL values
      val e = intercept[Exception] {
        sql("ALTER TABLE T ALTER COLUMN value SET NOT NULL")
      }
      Assertions.assertTrue(
        e.getMessage.contains("NULL values") ||
          e.getMessage.contains("existing rows"),
        s"Expected NULL values error but got: ${e.getMessage}")

      // Data should remain unchanged
      checkAnswer(
        sql("SELECT * FROM T ORDER BY id"),
        Row(1, 100) :: Row(2, null) :: Row(3, 200) :: Nil
      )

      // Column should still be nullable
      Assertions.assertTrue(spark.table("T").schema("value").nullable)
    }
  }

  test("Paimon DDL: SET NOT NULL succeeds on valid data") {
    withTable("T") {
      sql("CREATE TABLE T (id INT, value INT) USING paimon")

      // Insert data without NULL values
      sql("INSERT INTO T VALUES (1, 100)")
      sql("INSERT INTO T VALUES (2, 200)")

      // SET NOT NULL should succeed
      sql("ALTER TABLE T ALTER COLUMN value SET NOT NULL")

      // Verify constraint is enforced
      val e = intercept[Exception] {
        sql("INSERT INTO T VALUES (3, null)")
      }
      Assertions.assertTrue(e.getMessage.contains("value appeared in non-nullable field"))

      // Column should be non-nullable now
      Assertions.assertFalse(spark.table("T").schema("value").nullable)
    }
  }

  test("Paimon DDL: SET NOT NULL then DROP NOT NULL on same column") {
    withTable("T") {
      sql("CREATE TABLE T (id INT, value INT) USING paimon")
      sql("INSERT INTO T VALUES (1, 100), (2, 200)")

      // Initially nullable
      Assertions.assertTrue(spark.table("T").schema("value").nullable)

      // SET NOT NULL
      sql("ALTER TABLE T ALTER COLUMN value SET NOT NULL")
      Assertions.assertFalse(spark.table("T").schema("value").nullable)

      // Verify enforcement
      val e = intercept[Exception] {
        sql("INSERT INTO T VALUES (3, null)")
      }
      Assertions.assertTrue(e.getMessage.contains("value appeared in non-nullable field"))

      // DROP NOT NULL on the same column
      sql("ALTER TABLE T ALTER COLUMN value DROP NOT NULL")
      Assertions.assertTrue(spark.table("T").schema("value").nullable)

      // Now NULL values can be inserted again
      sql("INSERT INTO T VALUES (3, null)")
      checkAnswer(
        sql("SELECT * FROM T ORDER BY id"),
        Row(1, 100) :: Row(2, 200) :: Row(3, null) :: Nil
      )
    }
  }

  test("Paimon DDL: SET NOT NULL on already NOT NULL column") {
    withTable("T") {
      sql("CREATE TABLE T (id INT NOT NULL, value INT) USING paimon")
      sql("INSERT INTO T VALUES (1, 100)")

      // 'id' is already NOT NULL, SET NOT NULL again should succeed gracefully
      sql("ALTER TABLE T ALTER COLUMN id SET NOT NULL")
      Assertions.assertFalse(spark.table("T").schema("id").nullable)

      // Data should be unchanged
      checkAnswer(
        sql("SELECT * FROM T ORDER BY id"),
        Row(1, 100) :: Nil
      )
    }
  }

  test("Paimon DDL: DROP NOT NULL on already nullable column") {
    withTable("T") {
      sql("CREATE TABLE T (id INT NOT NULL, value INT) USING paimon")
      sql("INSERT INTO T VALUES (1, 100)")

      // 'value' is already nullable, DROP NOT NULL should succeed gracefully
      sql("ALTER TABLE T ALTER COLUMN value DROP NOT NULL")
      Assertions.assertTrue(spark.table("T").schema("value").nullable)

      // Data should be unchanged
      checkAnswer(
        sql("SELECT * FROM T ORDER BY id"),
        Row(1, 100) :: Nil
      )
    }
  }

  test("Paimon DDL: SET NOT NULL on primary key table non-PK column") {
    withTable("T") {
      sql("""CREATE TABLE T (id INT, name STRING, value INT)
            |TBLPROPERTIES ('primary-key' = 'id')
            |""".stripMargin)

      // PK column is already NOT NULL
      Assertions.assertFalse(spark.table("T").schema("id").nullable)
      Assertions.assertTrue(spark.table("T").schema("value").nullable)

      // Insert valid data
      sql("INSERT INTO T VALUES (1, 'Alice', 100)")
      sql("INSERT INTO T VALUES (2, 'Bob', 200)")

      // SET NOT NULL on non-PK column
      sql("ALTER TABLE T ALTER COLUMN value SET NOT NULL")
      Assertions.assertFalse(spark.table("T").schema("value").nullable)

      // Verify enforcement
      val e = intercept[Exception] {
        sql("INSERT INTO T VALUES (3, 'Charlie', null)")
      }
      Assertions.assertTrue(e.getMessage.contains("value appeared in non-nullable field"))

      // Valid insert should succeed
      sql("INSERT INTO T VALUES (3, 'Charlie', 300)")
      checkAnswer(
        sql("SELECT * FROM T ORDER BY id"),
        Row(1, "Alice", 100) :: Row(2, "Bob", 200) :: Row(3, "Charlie", 300) :: Nil
      )
    }
  }

  test("Paimon DDL: SET NOT NULL on partitioned table") {
    withTable("T") {
      sql("""CREATE TABLE T (id INT, value INT, pt STRING)
            |USING paimon
            |PARTITIONED BY (pt)
            |""".stripMargin)

      sql("INSERT INTO T VALUES (1, 100, 'p1'), (2, 200, 'p2')")

      // SET NOT NULL on data column
      sql("ALTER TABLE T ALTER COLUMN value SET NOT NULL")
      Assertions.assertFalse(spark.table("T").schema("value").nullable)

      // Verify enforcement across partitions
      val e1 = intercept[Exception] {
        sql("INSERT INTO T VALUES (3, null, 'p1')")
      }
      Assertions.assertTrue(e1.getMessage.contains("value appeared in non-nullable field"))

      val e2 = intercept[Exception] {
        sql("INSERT INTO T VALUES (4, null, 'p2')")
      }
      Assertions.assertTrue(e2.getMessage.contains("value appeared in non-nullable field"))

      // Valid inserts should succeed
      sql("INSERT INTO T VALUES (3, 300, 'p1'), (4, 400, 'p2')")
      checkAnswer(
        sql("SELECT * FROM T ORDER BY id"),
        Row(1, 100, "p1") :: Row(2, 200, "p2") :: Row(3, 300, "p1") :: Row(4, 400, "p2") :: Nil
      )
    }
  }

  test("Paimon DDL: SET NOT NULL with batch INSERT containing NULLs") {
    withTable("T") {
      sql("CREATE TABLE T (id INT, value INT) USING paimon")
      sql("INSERT INTO T VALUES (1, 100)")

      sql("ALTER TABLE T ALTER COLUMN value SET NOT NULL")

      // Batch insert where some rows have NULL should fail
      val e = intercept[Exception] {
        sql("INSERT INTO T VALUES (2, 200), (3, null), (4, 400)")
      }
      Assertions.assertTrue(e.getMessage.contains("value appeared in non-nullable field"))

      // All valid batch insert should succeed
      sql("INSERT INTO T VALUES (2, 200), (3, 300), (4, 400)")
      checkAnswer(
        sql("SELECT * FROM T ORDER BY id"),
        Row(1, 100) :: Row(2, 200) :: Row(3, 300) :: Row(4, 400) :: Nil
      )
    }
  }

  test("Paimon DDL: SET NOT NULL on multiple columns") {
    withTable("T") {
      sql("CREATE TABLE T (id INT, col1 INT, col2 STRING, col3 INT) USING paimon")
      sql("INSERT INTO T VALUES (1, 10, 'a', 100)")

      // All columns initially nullable (except id which is nullable too here)
      Assertions.assertTrue(spark.table("T").schema("col1").nullable)
      Assertions.assertTrue(spark.table("T").schema("col2").nullable)
      Assertions.assertTrue(spark.table("T").schema("col3").nullable)

      // SET NOT NULL on multiple columns sequentially
      sql("ALTER TABLE T ALTER COLUMN col1 SET NOT NULL")
      sql("ALTER TABLE T ALTER COLUMN col3 SET NOT NULL")

      Assertions.assertFalse(spark.table("T").schema("col1").nullable)
      Assertions.assertTrue(spark.table("T").schema("col2").nullable) // unchanged
      Assertions.assertFalse(spark.table("T").schema("col3").nullable)

      // NULL in col1 should fail
      val e1 = intercept[Exception] {
        sql("INSERT INTO T VALUES (2, null, 'b', 200)")
      }
      Assertions.assertTrue(e1.getMessage.contains("value appeared in non-nullable field"))

      // NULL in col2 (still nullable) should succeed
      sql("INSERT INTO T VALUES (2, 20, null, 200)")

      // NULL in col3 should fail
      val e2 = intercept[Exception] {
        sql("INSERT INTO T VALUES (3, 30, 'c', null)")
      }
      Assertions.assertTrue(e2.getMessage.contains("value appeared in non-nullable field"))

      checkAnswer(
        sql("SELECT * FROM T ORDER BY id"),
        Row(1, 10, "a", 100) :: Row(2, 20, null, 200) :: Nil
      )
    }
  }

  test("Paimon DDL: SET NOT NULL after removing NULL data") {
    withTable("T") {
      sql("CREATE TABLE T (id INT, value INT) USING paimon TBLPROPERTIES ('primary-key' = 'id')")

      // Insert data including NULL values
      sql("INSERT INTO T VALUES (1, 100)")
      sql("INSERT INTO T VALUES (2, null)")
      sql("INSERT INTO T VALUES (3, 200)")

      // SET NOT NULL should fail with existing NULLs
      val e = intercept[Exception] {
        sql("ALTER TABLE T ALTER COLUMN value SET NOT NULL")
      }
      Assertions.assertTrue(
        e.getMessage.contains("NULL values") ||
          e.getMessage.contains("existing rows"),
        s"Expected NULL values error but got: ${e.getMessage}")

      // Remove the row with NULL value by overwriting with non-null value
      sql("INSERT INTO T VALUES (2, 150)")

      // Now SET NOT NULL should succeed
      sql("ALTER TABLE T ALTER COLUMN value SET NOT NULL")
      Assertions.assertFalse(spark.table("T").schema("value").nullable)

      // Verify enforcement
      val e2 = intercept[Exception] {
        sql("INSERT INTO T VALUES (4, null)")
      }
      Assertions.assertTrue(e2.getMessage.contains("value appeared in non-nullable field"))
    }
  }

  test("Paimon DDL: add and drop CHECK constraint") {
    withTable("T") {
      sql("CREATE TABLE T (id INT, salary INT, name STRING) USING paimon")

      // Add CHECK constraint
      sql("ALTER TABLE T ADD CONSTRAINT salary_check CHECK (salary > 0)")

      // Verify constraint is stored in table options
      val table = loadTable("T")
      val options = table.options()
      Assertions.assertTrue(options.containsKey("constraint.check.salary_check"))
      Assertions.assertEquals("salary > 0", options.get("constraint.check.salary_check"))

      // Add another constraint
      sql("ALTER TABLE T ADD CONSTRAINT name_check CHECK (name IS NOT NULL)")
      val table2 = loadTable("T")
      val options2 = table2.options()
      Assertions.assertTrue(options2.containsKey("constraint.check.name_check"))
      Assertions.assertEquals("name IS NOT NULL", options2.get("constraint.check.name_check"))

      // Try to add duplicate constraint
      val e = intercept[Exception] {
        sql("ALTER TABLE T ADD CONSTRAINT salary_check CHECK (salary > 100)")
      }
      Assertions.assertTrue(e.getMessage.contains("already exists"))

      // Drop constraint
      sql("ALTER TABLE T DROP CONSTRAINT salary_check")
      val table3 = loadTable("T")
      val options3 = table3.options()
      Assertions.assertFalse(options3.containsKey("constraint.check.salary_check"))
      Assertions.assertTrue(options3.containsKey("constraint.check.name_check"))

      // Try to drop non-existing constraint
      val e2 = intercept[Exception] {
        sql("ALTER TABLE T DROP CONSTRAINT non_existing")
      }
      Assertions.assertTrue(e2.getMessage.contains("does not exist"))

      // Drop with IF EXISTS - should not throw error
      sql("ALTER TABLE T DROP CONSTRAINT IF EXISTS non_existing")

      // Drop remaining constraint
      sql("ALTER TABLE T DROP CONSTRAINT name_check")
      val table4 = loadTable("T")
      val options4 = table4.options()
      Assertions.assertFalse(options4.containsKey("constraint.check.name_check"))
    }
  }

  test("Paimon DDL: CHECK constraint with complex expressions") {
    withTable("T") {
      sql("CREATE TABLE T (id INT, birthDate STRING, salary INT) USING paimon")

      // Add constraint with date comparison
      sql("ALTER TABLE T ADD CONSTRAINT dateWithinRange CHECK (birthDate > '1900-01-01')")

      val table = loadTable("T")
      val options = table.options()
      Assertions.assertTrue(options.containsKey("constraint.check.dateWithinRange"))
      Assertions.assertEquals(
        "birthDate > '1900-01-01'",
        options.get("constraint.check.dateWithinRange"))

      // Add constraint with AND/OR
      sql("ALTER TABLE T ADD CONSTRAINT salary_range CHECK (salary >= 0 AND salary <= 1000000)")

      val table2 = loadTable("T")
      val options2 = table2.options()
      Assertions.assertTrue(options2.containsKey("constraint.check.salary_range"))
      Assertions.assertEquals(
        "salary >= 0 AND salary <= 1000000",
        options2.get("constraint.check.salary_range"))
    }
  }

  test("Paimon DDL: CHECK constraint enforcement on INSERT") {
    withTable("T") {
      sql("CREATE TABLE T (id INT, salary INT, name STRING) USING paimon")
      sql("ALTER TABLE T ADD CONSTRAINT salary_positive CHECK (salary > 0)")

      // Valid insert should succeed
      sql("INSERT INTO T VALUES (1, 100, 'Alice')")
      checkAnswer(
        sql("SELECT * FROM T ORDER BY id"),
        Row(1, 100, "Alice") :: Nil
      )

      // Invalid insert should fail: salary = 0 violates constraint
      val e1 = intercept[Exception] {
        sql("INSERT INTO T VALUES (2, 0, 'Bob')")
      }
      Assertions.assertTrue(
        e1.getMessage.contains("CHECK constraint") &&
          e1.getMessage.contains("salary_positive"))

      // Invalid insert should fail: salary = -10 violates constraint
      val e2 = intercept[Exception] {
        sql("INSERT INTO T VALUES (3, -10, 'Charlie')")
      }
      Assertions.assertTrue(
        e2.getMessage.contains("CHECK constraint") &&
          e2.getMessage.contains("salary_positive"))

      // Data should remain unchanged after failed inserts
      checkAnswer(
        sql("SELECT * FROM T ORDER BY id"),
        Row(1, 100, "Alice") :: Nil
      )
    }
  }

  test("Paimon DDL: CHECK constraint enforcement with multiple constraints") {
    withTable("T") {
      sql("CREATE TABLE T (id INT, age INT, salary INT) USING paimon")
      sql("ALTER TABLE T ADD CONSTRAINT age_valid CHECK (age >= 0 AND age <= 150)")
      sql("ALTER TABLE T ADD CONSTRAINT salary_positive CHECK (salary > 0)")

      // Valid insert should succeed
      sql("INSERT INTO T VALUES (1, 25, 5000)")
      checkAnswer(
        sql("SELECT * FROM T ORDER BY id"),
        Row(1, 25, 5000) :: Nil
      )

      // Violate first constraint (age)
      val e1 = intercept[Exception] {
        sql("INSERT INTO T VALUES (2, 200, 5000)")
      }
      Assertions.assertTrue(e1.getMessage.contains("CHECK constraint"))

      // Violate second constraint (salary)
      val e2 = intercept[Exception] {
        sql("INSERT INTO T VALUES (3, 30, -100)")
      }
      Assertions.assertTrue(e2.getMessage.contains("CHECK constraint"))

      // Data should remain unchanged
      checkAnswer(
        sql("SELECT * FROM T ORDER BY id"),
        Row(1, 25, 5000) :: Nil
      )
    }
  }

  test("Paimon DDL: CHECK constraint with NULL values") {
    withTable("T") {
      sql("CREATE TABLE T (id INT, value INT) USING paimon")
      sql("ALTER TABLE T ADD CONSTRAINT value_positive CHECK (value > 0)")

      // NULL values should violate CHECK constraint (stricter than SQL standard,
      // which treats NULL as "unknown" and does not consider it a violation)
      val e = intercept[Exception] {
        sql("INSERT INTO T VALUES (1, null)")
      }
      Assertions.assertTrue(e.getMessage.contains("CHECK constraint"))

      // Valid insert should succeed
      sql("INSERT INTO T VALUES (2, 10)")
      checkAnswer(
        sql("SELECT * FROM T ORDER BY id"),
        Row(2, 10) :: Nil
      )
    }
  }

  test("Paimon DDL: CHECK constraint enforcement with V2 Write") {
    for (useV2Write <- Seq("true", "false")) {
      withSparkSQLConf("spark.paimon.write.use-v2-write" -> useV2Write) {
        withTable("T") {
          sql("CREATE TABLE T (id INT, salary INT, name STRING) USING paimon")
          sql("ALTER TABLE T ADD CONSTRAINT salary_positive CHECK (salary > 0)")

          // Valid insert should succeed
          sql("INSERT INTO T VALUES (1, 100, 'Alice')")
          checkAnswer(
            sql("SELECT * FROM T ORDER BY id"),
            Row(1, 100, "Alice") :: Nil
          )

          // Invalid insert should fail: salary = 0 violates constraint
          val e1 = intercept[Exception] {
            sql("INSERT INTO T VALUES (2, 0, 'Bob')")
          }
          Assertions.assertTrue(
            e1.getMessage.contains("CHECK constraint") &&
              e1.getMessage.contains("salary_positive"),
            s"Expected CHECK constraint error but got: ${e1.getMessage}")

          // Invalid insert should fail: salary = -10 violates constraint
          val e2 = intercept[Exception] {
            sql("INSERT INTO T VALUES (3, -10, 'Charlie')")
          }
          Assertions.assertTrue(
            e2.getMessage.contains("CHECK constraint") &&
              e2.getMessage.contains("salary_positive"),
            s"Expected CHECK constraint error but got: ${e2.getMessage}")

          // Data should remain unchanged after failed inserts
          checkAnswer(
            sql("SELECT * FROM T ORDER BY id"),
            Row(1, 100, "Alice") :: Nil
          )
        }
      }
    }
  }

  test("Paimon DDL: CHECK constraint enforcement with multiple constraints using V2 Write") {
    for (useV2Write <- Seq("true", "false")) {
      withSparkSQLConf("spark.paimon.write.use-v2-write" -> useV2Write) {
        withTable("T") {
          sql("CREATE TABLE T (id INT, age INT, salary INT) USING paimon")
          sql("ALTER TABLE T ADD CONSTRAINT age_valid CHECK (age >= 0 AND age <= 150)")
          sql("ALTER TABLE T ADD CONSTRAINT salary_positive CHECK (salary > 0)")

          // Valid insert should succeed
          sql("INSERT INTO T VALUES (1, 25, 5000)")
          checkAnswer(
            sql("SELECT * FROM T ORDER BY id"),
            Row(1, 25, 5000) :: Nil
          )

          // Violate first constraint (age)
          val e1 = intercept[Exception] {
            sql("INSERT INTO T VALUES (2, 200, 5000)")
          }
          Assertions.assertTrue(
            e1.getMessage.contains("CHECK constraint"),
            s"Expected CHECK constraint error but got: ${e1.getMessage}")

          // Violate second constraint (salary)
          val e2 = intercept[Exception] {
            sql("INSERT INTO T VALUES (3, 30, -100)")
          }
          Assertions.assertTrue(
            e2.getMessage.contains("CHECK constraint"),
            s"Expected CHECK constraint error but got: ${e2.getMessage}")

          // Data should remain unchanged
          checkAnswer(
            sql("SELECT * FROM T ORDER BY id"),
            Row(1, 25, 5000) :: Nil
          )
        }
      }
    }
  }

  test("Paimon DDL: CHECK constraint with NULL values using V2 Write") {
    for (useV2Write <- Seq("true", "false")) {
      withSparkSQLConf("spark.paimon.write.use-v2-write" -> useV2Write) {
        withTable("T") {
          sql("CREATE TABLE T (id INT, value INT) USING paimon")
          sql("ALTER TABLE T ADD CONSTRAINT value_positive CHECK (value > 0)")

          // NULL values should violate CHECK constraint (stricter than SQL standard,
          // which treats NULL as "unknown" and does not consider it a violation)
          val e = intercept[Exception] {
            sql("INSERT INTO T VALUES (1, null)")
          }
          Assertions.assertTrue(
            e.getMessage.contains("CHECK constraint"),
            s"Expected CHECK constraint error but got: ${e.getMessage}")

          // Valid insert should succeed
          sql("INSERT INTO T VALUES (2, 10)")
          checkAnswer(
            sql("SELECT * FROM T ORDER BY id"),
            Row(2, 10) :: Nil
          )
        }
      }
    }
  }

  test("Paimon DDL: ADD CHECK constraint validates existing data") {
    withTable("T") {
      sql("CREATE TABLE T (id INT, salary INT) USING paimon")

      // Insert some data first (including invalid data)
      sql("INSERT INTO T VALUES (1, 100)")
      sql("INSERT INTO T VALUES (2, -50)")
      sql("INSERT INTO T VALUES (3, 200)")

      // Adding constraint should fail because existing data violates it
      val e = intercept[Exception] {
        sql("ALTER TABLE T ADD CONSTRAINT salary_positive CHECK (salary > 0)")
      }
      Assertions.assertTrue(
        e.getMessage.contains("existing rows violate") ||
          e.getMessage.contains("CHECK constraint"),
        s"Expected existing data violation error but got: ${e.getMessage}"
      )

      // Table should not have the constraint (verify by inserting invalid data)
      // Note: This might succeed if constraint was not added
      checkAnswer(
        sql("SELECT * FROM T ORDER BY id"),
        Row(1, 100) :: Row(2, -50) :: Row(3, 200) :: Nil
      )
    }
  }

  test("Paimon DDL: ADD CHECK constraint succeeds on valid existing data") {
    withTable("T") {
      sql("CREATE TABLE T (id INT, salary INT) USING paimon")

      // Insert valid data only
      sql("INSERT INTO T VALUES (1, 100)")
      sql("INSERT INTO T VALUES (2, 200)")

      // Adding constraint should succeed
      sql("ALTER TABLE T ADD CONSTRAINT salary_positive CHECK (salary > 0)")

      // Verify constraint is enforced for new inserts
      val e = intercept[Exception] {
        sql("INSERT INTO T VALUES (3, -50)")
      }
      Assertions.assertTrue(
        e.getMessage.contains("CHECK constraint"),
        s"Expected CHECK constraint error but got: ${e.getMessage}")

      // Original data should remain
      checkAnswer(
        sql("SELECT * FROM T ORDER BY id"),
        Row(1, 100) :: Row(2, 200) :: Nil
      )
    }
  }

  test("Paimon DDL: CHECK constraint rejects non-deterministic expressions") {
    withTable("T") {
      sql("CREATE TABLE T (id INT, value DOUBLE) USING paimon")

      // rand() is non-deterministic and should be rejected
      val e1 = intercept[Exception] {
        sql("ALTER TABLE T ADD CONSTRAINT random_check CHECK (value > rand())")
      }
      Assertions.assertTrue(
        e1.getMessage.contains("deterministic") ||
          e1.getMessage.contains("non-deterministic"),
        s"Expected deterministic error but got: ${e1.getMessage}")

      // uuid() is non-deterministic and should be rejected
      val e2 = intercept[Exception] {
        sql("ALTER TABLE T ADD CONSTRAINT uuid_check CHECK (id > 0 OR uuid() IS NOT NULL)")
      }
      Assertions.assertTrue(
        e2.getMessage.contains("deterministic") ||
          e2.getMessage.contains("non-deterministic"),
        s"Expected deterministic error but got: ${e2.getMessage}")

      // Deterministic expressions should be allowed
      sql("ALTER TABLE T ADD CONSTRAINT value_positive CHECK (value > 0)")

      // Verify constraint was added
      val options = loadTable("T").options()
      Assertions.assertTrue(options.containsKey("constraint.check.value_positive"))
    }
  }

  test("Paimon DDL: DROP CHECK constraint removes enforcement") {
    withTable("T") {
      sql("CREATE TABLE T (id INT, salary INT) USING paimon")
      sql("ALTER TABLE T ADD CONSTRAINT salary_positive CHECK (salary > 0)")

      // Verify constraint is enforced
      val e = intercept[Exception] {
        sql("INSERT INTO T VALUES (1, -10)")
      }
      Assertions.assertTrue(e.getMessage.contains("CHECK constraint"))

      // Drop constraint
      sql("ALTER TABLE T DROP CONSTRAINT salary_positive")

      // Previously-blocked insert should now succeed
      sql("INSERT INTO T VALUES (1, -10)")
      sql("INSERT INTO T VALUES (2, 0)")
      sql("INSERT INTO T VALUES (3, 100)")
      checkAnswer(
        sql("SELECT * FROM T ORDER BY id"),
        Row(1, -10) :: Row(2, 0) :: Row(3, 100) :: Nil
      )
    }
  }

  test("Paimon DDL: CHECK constraint with batch INSERT containing mixed valid/invalid rows") {
    withTable("T") {
      sql("CREATE TABLE T (id INT, salary INT, name STRING) USING paimon")
      sql("ALTER TABLE T ADD CONSTRAINT salary_positive CHECK (salary > 0)")

      // Batch insert with some rows violating constraint
      val e = intercept[Exception] {
        sql("INSERT INTO T VALUES (1, 100, 'Alice'), (2, -5, 'Bob'), (3, 200, 'Charlie')")
      }
      Assertions.assertTrue(
        e.getMessage.contains("CHECK constraint"),
        s"Expected CHECK constraint error but got: ${e.getMessage}")

      // All valid batch insert should succeed
      sql("INSERT INTO T VALUES (1, 100, 'Alice'), (2, 50, 'Bob'), (3, 200, 'Charlie')")
      checkAnswer(
        sql("SELECT * FROM T ORDER BY id"),
        Row(1, 100, "Alice") :: Row(2, 50, "Bob") :: Row(3, 200, "Charlie") :: Nil
      )
    }
  }

  test("Paimon DDL: CHECK constraint with multi-column cross reference") {
    withTable("T") {
      sql("CREATE TABLE T (id INT, min_val INT, max_val INT) USING paimon")
      sql("ALTER TABLE T ADD CONSTRAINT range_valid CHECK (min_val < max_val)")

      // Valid: min_val < max_val
      sql("INSERT INTO T VALUES (1, 10, 100)")
      checkAnswer(
        sql("SELECT * FROM T ORDER BY id"),
        Row(1, 10, 100) :: Nil
      )

      // Invalid: min_val > max_val
      val e1 = intercept[Exception] {
        sql("INSERT INTO T VALUES (2, 100, 10)")
      }
      Assertions.assertTrue(
        e1.getMessage.contains("CHECK constraint"),
        s"Expected CHECK constraint error but got: ${e1.getMessage}")

      // Invalid: min_val == max_val
      val e2 = intercept[Exception] {
        sql("INSERT INTO T VALUES (3, 50, 50)")
      }
      Assertions.assertTrue(
        e2.getMessage.contains("CHECK constraint"),
        s"Expected CHECK constraint error but got: ${e2.getMessage}")

      // Data should be unchanged
      checkAnswer(
        sql("SELECT * FROM T ORDER BY id"),
        Row(1, 10, 100) :: Nil
      )
    }
  }

  test("Paimon DDL: CHECK constraint with built-in functions") {
    withTable("T") {
      sql("CREATE TABLE T (id INT, name STRING, value INT) USING paimon")

      // Constraint using LENGTH function
      sql("ALTER TABLE T ADD CONSTRAINT name_length CHECK (LENGTH(name) > 0)")
      // Constraint using ABS function
      sql("ALTER TABLE T ADD CONSTRAINT value_range CHECK (ABS(value) < 1000)")

      // Valid insert
      sql("INSERT INTO T VALUES (1, 'Alice', 500)")
      checkAnswer(
        sql("SELECT * FROM T ORDER BY id"),
        Row(1, "Alice", 500) :: Nil
      )

      // Invalid: empty name (LENGTH = 0)
      val e1 = intercept[Exception] {
        sql("INSERT INTO T VALUES (2, '', 100)")
      }
      Assertions.assertTrue(
        e1.getMessage.contains("CHECK constraint"),
        s"Expected CHECK constraint error but got: ${e1.getMessage}")

      // Invalid: ABS(value) >= 1000
      val e2 = intercept[Exception] {
        sql("INSERT INTO T VALUES (3, 'Bob', 1500)")
      }
      Assertions.assertTrue(
        e2.getMessage.contains("CHECK constraint"),
        s"Expected CHECK constraint error but got: ${e2.getMessage}")

      // Also test negative large value
      val e3 = intercept[Exception] {
        sql("INSERT INTO T VALUES (4, 'Charlie', -1500)")
      }
      Assertions.assertTrue(
        e3.getMessage.contains("CHECK constraint"),
        s"Expected CHECK constraint error but got: ${e3.getMessage}")
    }
  }

  test("Paimon DDL: CHECK constraint enforcement on INSERT OVERWRITE") {
    withTable("T") {
      sql("CREATE TABLE T (id INT, salary INT, pt STRING) USING paimon PARTITIONED BY (pt)")
      sql("ALTER TABLE T ADD CONSTRAINT salary_positive CHECK (salary > 0)")

      // Valid INSERT
      sql("INSERT INTO T VALUES (1, 100, 'p1')")

      // Valid INSERT OVERWRITE should succeed
      sql("INSERT OVERWRITE T VALUES (1, 200, 'p1')")
      checkAnswer(
        sql("SELECT * FROM T ORDER BY id"),
        Row(1, 200, "p1") :: Nil
      )

      // Invalid INSERT OVERWRITE should fail
      val e = intercept[Exception] {
        sql("INSERT OVERWRITE T VALUES (1, -50, 'p1')")
      }
      Assertions.assertTrue(
        e.getMessage.contains("CHECK constraint"),
        s"Expected CHECK constraint error but got: ${e.getMessage}")

      // Data should remain unchanged
      checkAnswer(
        sql("SELECT * FROM T ORDER BY id"),
        Row(1, 200, "p1") :: Nil
      )
    }
  }

  test("Paimon DDL: CHECK constraint enforcement on INSERT INTO SELECT") {
    withTable("T", "source") {
      sql("CREATE TABLE source (id INT, salary INT, name STRING) USING paimon")
      sql("CREATE TABLE T (id INT, salary INT, name STRING) USING paimon")
      sql("ALTER TABLE T ADD CONSTRAINT salary_positive CHECK (salary > 0)")

      // Insert valid source data
      sql("INSERT INTO source VALUES (1, 100, 'Alice'), (2, 200, 'Bob')")

      // INSERT INTO SELECT with all valid data should succeed
      sql("INSERT INTO T SELECT * FROM source")
      checkAnswer(
        sql("SELECT * FROM T ORDER BY id"),
        Row(1, 100, "Alice") :: Row(2, 200, "Bob") :: Nil
      )

      // Insert invalid source data
      sql("INSERT INTO source VALUES (3, -50, 'Charlie')")

      // INSERT INTO SELECT with some invalid data should fail
      val e = intercept[Exception] {
        sql("INSERT INTO T SELECT * FROM source WHERE id = 3")
      }
      Assertions.assertTrue(
        e.getMessage.contains("CHECK constraint"),
        s"Expected CHECK constraint error but got: ${e.getMessage}")
    }
  }

  test("Paimon DDL: CHECK constraint on primary key table") {
    withTable("T") {
      sql("""CREATE TABLE T (id INT, salary INT, name STRING)
            |TBLPROPERTIES ('primary-key' = 'id')
            |""".stripMargin)
      sql("ALTER TABLE T ADD CONSTRAINT salary_positive CHECK (salary > 0)")

      // Valid insert
      sql("INSERT INTO T VALUES (1, 100, 'Alice')")
      checkAnswer(
        sql("SELECT * FROM T ORDER BY id"),
        Row(1, 100, "Alice") :: Nil
      )

      // Invalid insert should fail
      val e = intercept[Exception] {
        sql("INSERT INTO T VALUES (2, -50, 'Bob')")
      }
      Assertions.assertTrue(
        e.getMessage.contains("CHECK constraint"),
        s"Expected CHECK constraint error but got: ${e.getMessage}")

      // Valid upsert (update existing row)
      sql("INSERT INTO T VALUES (1, 200, 'Alice Updated')")
      checkAnswer(
        sql("SELECT * FROM T ORDER BY id"),
        Row(1, 200, "Alice Updated") :: Nil
      )
    }
  }

  test("Paimon DDL: CHECK constraint on partitioned table") {
    withTable("T") {
      sql("""CREATE TABLE T (id INT, salary INT, pt STRING)
            |USING paimon
            |PARTITIONED BY (pt)
            |""".stripMargin)
      sql("ALTER TABLE T ADD CONSTRAINT salary_positive CHECK (salary > 0)")

      // Valid insert across partitions
      sql("INSERT INTO T VALUES (1, 100, 'p1')")
      sql("INSERT INTO T VALUES (2, 200, 'p2')")
      checkAnswer(
        sql("SELECT * FROM T ORDER BY id"),
        Row(1, 100, "p1") :: Row(2, 200, "p2") :: Nil
      )

      // Invalid insert on any partition should fail
      val e1 = intercept[Exception] {
        sql("INSERT INTO T VALUES (3, -50, 'p1')")
      }
      Assertions.assertTrue(
        e1.getMessage.contains("CHECK constraint"),
        s"Expected CHECK constraint error but got: ${e1.getMessage}")

      val e2 = intercept[Exception] {
        sql("INSERT INTO T VALUES (4, 0, 'p2')")
      }
      Assertions.assertTrue(
        e2.getMessage.contains("CHECK constraint"),
        s"Expected CHECK constraint error but got: ${e2.getMessage}")

      // Data should remain unchanged
      checkAnswer(
        sql("SELECT * FROM T ORDER BY id"),
        Row(1, 100, "p1") :: Row(2, 200, "p2") :: Nil
      )
    }
  }

  test("Paimon DDL: CHECK constraint with fully qualified catalog table name") {
    withTable("T") {
      sql("CREATE TABLE T (id INT, salary INT, name STRING) USING paimon")
      sql("ALTER TABLE T ADD CONSTRAINT salary_positive CHECK (salary > 0)")

      // Verify constraint is stored
      val table = loadTable("T")
      Assertions.assertTrue(table.options().containsKey("constraint.check.salary_positive"))

      // Valid insert using fully qualified name
      sql(s"INSERT INTO paimon.$dbName0.T VALUES (1, 100, 'Alice')")
      checkAnswer(
        sql("SELECT * FROM T ORDER BY id"),
        Row(1, 100, "Alice") :: Nil
      )

      // Invalid insert should still be caught
      val e = intercept[Exception] {
        sql(s"INSERT INTO paimon.$dbName0.T VALUES (2, -50, 'Bob')")
      }
      Assertions.assertTrue(
        e.getMessage.contains("CHECK constraint"),
        s"Expected CHECK constraint error but got: ${e.getMessage}")
    }
  }

  test("Paimon DDL: CHECK constraint with INSERT INTO SELECT column reorder") {
    withTable("T", "source") {
      sql("CREATE TABLE T (id INT, salary INT, name STRING) USING paimon")
      sql("ALTER TABLE T ADD CONSTRAINT salary_positive CHECK (salary > 0)")

      sql("CREATE TABLE source (name STRING, id INT, salary INT) USING paimon")
      sql("INSERT INTO source VALUES ('Alice', 1, 100)")
      sql("INSERT INTO source VALUES ('Bob', 2, -50)")

      // INSERT INTO SELECT with reordered columns - valid data
      sql("INSERT INTO T SELECT id, salary, name FROM source WHERE salary > 0")
      checkAnswer(
        sql("SELECT * FROM T ORDER BY id"),
        Row(1, 100, "Alice") :: Nil
      )

      // INSERT INTO SELECT with reordered columns - invalid data should fail
      val e = intercept[Exception] {
        sql("INSERT INTO T SELECT id, salary, name FROM source WHERE id = 2")
      }
      Assertions.assertTrue(
        e.getMessage.contains("CHECK constraint"),
        s"Expected CHECK constraint error but got: ${e.getMessage}")
    }
  }

  test("Paimon DDL: CHECK constraint persists through table reload") {
    withTable("T") {
      sql("CREATE TABLE T (id INT, salary INT) USING paimon")
      sql("ALTER TABLE T ADD CONSTRAINT salary_positive CHECK (salary > 0)")

      // Valid insert
      sql("INSERT INTO T VALUES (1, 100)")

      // Invalidate table cache and verify constraint still works
      spark.catalog.refreshTable(s"paimon.$dbName0.T")

      // Constraint should still be enforced after reload
      val e = intercept[Exception] {
        sql("INSERT INTO T VALUES (2, -50)")
      }
      Assertions.assertTrue(
        e.getMessage.contains("CHECK constraint"),
        s"Expected CHECK constraint error after reload but got: ${e.getMessage}")

      // Valid insert after reload should still work
      sql("INSERT INTO T VALUES (3, 200)")
      checkAnswer(
        sql("SELECT * FROM T ORDER BY id"),
        Row(1, 100) :: Row(3, 200) :: Nil
      )
    }
  }

  test("Paimon DDL: DROP CHECK constraint with IF EXISTS on non-existing constraint") {
    withTable("T") {
      sql("CREATE TABLE T (id INT, salary INT) USING paimon")

      // DROP CONSTRAINT IF EXISTS should not throw on non-existing constraint
      sql("ALTER TABLE T DROP CONSTRAINT IF EXISTS non_existing_constraint")

      // DROP CONSTRAINT without IF EXISTS should throw
      val e = intercept[Exception] {
        sql("ALTER TABLE T DROP CONSTRAINT non_existing_constraint")
      }
      Assertions.assertTrue(
        e.getMessage.contains("does not exist"),
        s"Expected 'does not exist' error but got: ${e.getMessage}")
    }
  }

  test("Paimon DDL: CHECK constraint with add and then immediate insert") {
    for (useV2Write <- Seq("true", "false")) {
      withSparkSQLConf("spark.paimon.write.use-v2-write" -> useV2Write) {
        withTable("T") {
          sql("CREATE TABLE T (id INT, salary INT, name STRING) USING paimon")

          // Add constraint and immediately try to insert valid data
          sql("ALTER TABLE T ADD CONSTRAINT salary_positive CHECK (salary > 0)")
          sql("INSERT INTO T VALUES (1, 100, 'Alice')")

          // Verify data
          checkAnswer(
            sql("SELECT * FROM T ORDER BY id"),
            Row(1, 100, "Alice") :: Nil
          )

          // Add a second constraint, then test both are enforced
          sql("ALTER TABLE T ADD CONSTRAINT name_not_empty CHECK (LENGTH(name) > 0)")

          // Violate first constraint
          val e1 = intercept[Exception] {
            sql("INSERT INTO T VALUES (2, -5, 'Bob')")
          }
          Assertions.assertTrue(
            e1.getMessage.contains("CHECK constraint"),
            s"Expected CHECK constraint error but got: ${e1.getMessage}")

          // Violate second constraint
          val e2 = intercept[Exception] {
            sql("INSERT INTO T VALUES (3, 100, '')")
          }
          Assertions.assertTrue(
            e2.getMessage.contains("CHECK constraint"),
            s"Expected CHECK constraint error but got: ${e2.getMessage}")

          // Both constraints satisfied
          sql("INSERT INTO T VALUES (4, 200, 'Charlie')")
          checkAnswer(
            sql("SELECT * FROM T ORDER BY id"),
            Row(1, 100, "Alice") :: Row(4, 200, "Charlie") :: Nil
          )
        }
      }
    }
  }

  test("Paimon DDL: CHECK constraint enforcement on UPDATE") {
    withTable("T") {
      sql("""CREATE TABLE T (id INT, salary INT, name STRING) USING paimon
            |TBLPROPERTIES ('primary-key' = 'id')
            |""".stripMargin)
      sql("ALTER TABLE T ADD CONSTRAINT salary_positive CHECK (salary > 0)")

      // Insert valid data
      sql("INSERT INTO T VALUES (1, 100, 'Alice'), (2, 200, 'Bob')")
      checkAnswer(
        sql("SELECT * FROM T ORDER BY id"),
        Row(1, 100, "Alice") :: Row(2, 200, "Bob") :: Nil
      )

      // UPDATE that keeps data valid should succeed
      sql("UPDATE T SET salary = 300 WHERE id = 1")
      checkAnswer(
        sql("SELECT * FROM T ORDER BY id"),
        Row(1, 300, "Alice") :: Row(2, 200, "Bob") :: Nil
      )

      // UPDATE that violates constraint should fail
      val e1 = intercept[Exception] {
        sql("UPDATE T SET salary = 0 WHERE id = 1")
      }
      Assertions.assertTrue(
        e1.getMessage.contains("CHECK constraint"),
        s"Expected CHECK constraint error but got: ${e1.getMessage}")

      // UPDATE with negative salary should fail
      val e2 = intercept[Exception] {
        sql("UPDATE T SET salary = -50 WHERE id = 2")
      }
      Assertions.assertTrue(
        e2.getMessage.contains("CHECK constraint"),
        s"Expected CHECK constraint error but got: ${e2.getMessage}")

      // Data should remain unchanged after failed updates
      checkAnswer(
        sql("SELECT * FROM T ORDER BY id"),
        Row(1, 300, "Alice") :: Row(2, 200, "Bob") :: Nil
      )
    }
  }

  test("Paimon DDL: CHECK constraint enforcement on MERGE INTO") {
    withTable("T", "source") {
      sql("""CREATE TABLE T (id INT, salary INT, name STRING) USING paimon
            |TBLPROPERTIES ('primary-key' = 'id')
            |""".stripMargin)
      sql("ALTER TABLE T ADD CONSTRAINT salary_positive CHECK (salary > 0)")

      sql("INSERT INTO T VALUES (1, 100, 'Alice'), (2, 200, 'Bob')")

      sql("CREATE TABLE source (id INT, salary INT, name STRING) USING paimon")

      // MERGE INSERT with valid data should succeed
      sql("INSERT INTO source VALUES (3, 300, 'Charlie')")
      sql("""MERGE INTO T
            |USING source AS s ON T.id = s.id
            |WHEN NOT MATCHED THEN INSERT *
            |""".stripMargin)
      checkAnswer(
        sql("SELECT * FROM T ORDER BY id"),
        Row(1, 100, "Alice") :: Row(2, 200, "Bob") :: Row(3, 300, "Charlie") :: Nil
      )

      // MERGE INSERT with invalid data should fail
      sql("DELETE FROM source WHERE true")
      sql("INSERT INTO source VALUES (4, -50, 'Dave')")
      val e1 = intercept[Exception] {
        sql("""MERGE INTO T
              |USING source AS s ON T.id = s.id
              |WHEN NOT MATCHED THEN INSERT *
              |""".stripMargin)
      }
      Assertions.assertTrue(
        e1.getMessage.contains("CHECK constraint"),
        s"Expected CHECK constraint error on MERGE INSERT but got: ${e1.getMessage}")

      // MERGE UPDATE with valid data should succeed
      sql("DELETE FROM source WHERE true")
      sql("INSERT INTO source VALUES (1, 500, 'Alice Updated')")
      sql("""MERGE INTO T
            |USING source AS s ON T.id = s.id
            |WHEN MATCHED THEN UPDATE SET salary = s.salary, name = s.name
            |""".stripMargin)
      checkAnswer(
        sql("SELECT * FROM T WHERE id = 1"),
        Row(1, 500, "Alice Updated") :: Nil
      )

      // MERGE UPDATE with invalid data should fail
      sql("DELETE FROM source WHERE true")
      sql("INSERT INTO source VALUES (2, -100, 'Bob Bad')")
      val e2 = intercept[Exception] {
        sql("""MERGE INTO T
              |USING source AS s ON T.id = s.id
              |WHEN MATCHED THEN UPDATE SET salary = s.salary, name = s.name
              |""".stripMargin)
      }
      Assertions.assertTrue(
        e2.getMessage.contains("CHECK constraint"),
        s"Expected CHECK constraint error on MERGE UPDATE but got: ${e2.getMessage}")

      // Data should remain unchanged after failed MERGE operations
      checkAnswer(
        sql("SELECT * FROM T ORDER BY id"),
        Row(1, 500, "Alice Updated") :: Row(2, 200, "Bob") :: Row(3, 300, "Charlie") :: Nil
      )
    }
  }

  test("Paimon DDL: CHECK constraint enforcement with Streaming Write - valid data") {
    withSparkSQLConf("spark.paimon.write.use-v2-write" -> "true") {
      withTable("T") {
        sql("CREATE TABLE T (id INT, value INT) USING paimon")
        sql("ALTER TABLE T ADD CONSTRAINT value_positive CHECK (value > 0)")

        withTempDir {
          checkpointDir =>
            val inputData = MemoryStream[(Int, Int)]
            val stream = inputData
              .toDS()
              .toDF("id", "value")
              .writeStream
              .option("checkpointLocation", checkpointDir.getCanonicalPath)
              .format("paimon")
              .toTable("T")

            try {
              inputData.addData((1, 100), (2, 200))
              stream.processAllAvailable()

              checkAnswer(
                sql("SELECT * FROM T ORDER BY id"),
                Row(1, 100) :: Row(2, 200) :: Nil
              )
            } finally {
              stream.stop()
            }
        }
      }
    }
  }

  test("Paimon DDL: CHECK constraint violation in Streaming Write fails") {
    withSparkSQLConf("spark.paimon.write.use-v2-write" -> "true") {
      withTable("T") {
        sql("CREATE TABLE T (id INT, value INT) USING paimon")
        sql("ALTER TABLE T ADD CONSTRAINT value_positive CHECK (value > 0)")

        withTempDir {
          checkpointDir =>
            val inputData = MemoryStream[(Int, Int)]
            val stream = inputData
              .toDS()
              .toDF("id", "value")
              .writeStream
              .option("checkpointLocation", checkpointDir.getCanonicalPath)
              .format("paimon")
              .toTable("T")

            try {
              inputData.addData((1, -100), (2, -200))
              val e = intercept[StreamingQueryException] {
                stream.processAllAvailable()
              }
              Assertions.assertTrue(
                e.getMessage.contains("CHECK constraint"),
                s"Expected CHECK constraint error but got: ${e.getMessage}")
            } finally {
              stream.stop()
            }
        }
      }
    }
  }

  test("Paimon DDL: CHECK constraint RENAME COLUMN updates constraint expression") {
    withTable("T") {
      sql("CREATE TABLE T (id INT, salary INT) USING paimon")
      sql("ALTER TABLE T ADD CONSTRAINT salary_positive CHECK (salary > 0)")

      // Verify original constraint
      val options1 = loadTable("T").options()
      Assertions.assertEquals("salary > 0", options1.get("constraint.check.salary_positive"))

      // Rename the column referenced by the constraint
      sql("ALTER TABLE T RENAME COLUMN salary TO income")

      // Verify constraint expression was updated
      val options2 = loadTable("T").options()
      Assertions.assertEquals("income > 0", options2.get("constraint.check.salary_positive"))

      // Verify constraint still works with the new column name
      sql("INSERT INTO T VALUES (1, 100)")
      val e = intercept[Exception] {
        sql("INSERT INTO T VALUES (2, -50)")
      }
      Assertions.assertTrue(
        e.getMessage.contains("CHECK constraint"),
        s"Expected CHECK constraint error but got: ${e.getMessage}")

      checkAnswer(sql("SELECT * FROM T ORDER BY id"), Row(1, 100) :: Nil)
    }
  }

  test("Paimon DDL: DROP COLUMN referenced by CHECK constraint is blocked") {
    withTable("T") {
      sql("CREATE TABLE T (id INT, salary INT, name STRING) USING paimon")
      sql("ALTER TABLE T ADD CONSTRAINT salary_positive CHECK (salary > 0)")

      // Dropping the column referenced by constraint should fail
      val e = intercept[Exception] {
        sql("ALTER TABLE T DROP COLUMN salary")
      }
      Assertions.assertTrue(
        e.getMessage.contains("Cannot drop column") &&
          e.getMessage.contains("salary_positive"),
        s"Expected constraint reference error but got: ${e.getMessage}"
      )

      // Dropping non-referenced column should succeed
      sql("ALTER TABLE T DROP COLUMN name")

      // Verify table schema after dropping non-referenced column
      val schema = spark.table("T").schema
      Assertions.assertEquals(2, schema.size)

      // Drop constraint first, then drop column should succeed
      sql("ALTER TABLE T DROP CONSTRAINT salary_positive")
      sql("ALTER TABLE T DROP COLUMN salary")

      // Verify final schema
      val finalSchema = spark.table("T").schema
      Assertions.assertEquals(1, finalSchema.size)
      Assertions.assertEquals("id", finalSchema.head.name)
    }
  }

  test("Paimon DDL: CHECK constraint RENAME nested COLUMN updates constraint expression") {
    withTable("T") {
      sql("CREATE TABLE T (id INT, info STRUCT<age: INT, name: STRING>) USING paimon")
      sql("ALTER TABLE T ADD CONSTRAINT age_positive CHECK (info.age > 0)")

      // Verify original constraint
      val options1 = loadTable("T").options()
      Assertions.assertEquals("info.age > 0", options1.get("constraint.check.age_positive"))

      // Rename the nested column referenced by the constraint
      sql("ALTER TABLE T RENAME COLUMN info.age TO user_age")

      // Verify constraint expression was updated
      val options2 = loadTable("T").options()
      Assertions.assertEquals("info.user_age > 0", options2.get("constraint.check.age_positive"))

      // Verify constraint still works with the new column name
      sql("INSERT INTO T VALUES (1, struct(25, 'Alice'))")
      val e = intercept[Exception] {
        sql("INSERT INTO T VALUES (2, struct(-5, 'Bob'))")
      }
      Assertions.assertTrue(
        e.getMessage.contains("CHECK constraint"),
        s"Expected CHECK constraint error but got: ${e.getMessage}")

      checkAnswer(sql("SELECT * FROM T ORDER BY id"), Row(1, Row(25, "Alice")) :: Nil)
    }
  }

  test("Paimon DDL: DROP nested COLUMN referenced by CHECK constraint is blocked") {
    withTable("T") {
      sql("CREATE TABLE T (id INT, pay STRUCT<salary: INT, bonus: INT>) USING paimon")
      sql("ALTER TABLE T ADD CONSTRAINT salary_positive CHECK (pay.salary > 0)")

      // Dropping the nested column referenced by constraint should fail
      val e = intercept[Exception] {
        sql("ALTER TABLE T DROP COLUMN pay.salary")
      }
      Assertions.assertTrue(
        e.getMessage.contains("Cannot drop column") &&
          e.getMessage.contains("salary_positive"),
        s"Expected constraint reference error but got: ${e.getMessage}"
      )

      // Dropping non-referenced nested column should succeed
      sql("ALTER TABLE T DROP COLUMN pay.bonus")

      // Constraint should still be enforced
      sql("INSERT INTO T VALUES (1, struct(100))")
      val e2 = intercept[Exception] {
        sql("INSERT INTO T VALUES (2, struct(-50))")
      }
      Assertions.assertTrue(
        e2.getMessage.contains("CHECK constraint"),
        s"Expected CHECK constraint error but got: ${e2.getMessage}")

      checkAnswer(sql("SELECT * FROM T ORDER BY id"), Row(1, Row(100)) :: Nil)
    }
  }

  test("Paimon DDL: DROP parent COLUMN of CHECK constraint nested reference is blocked") {
    withTable("T") {
      sql("CREATE TABLE T (id INT, info STRUCT<val: INT, tag: STRING>, extra STRING) USING paimon")
      sql("ALTER TABLE T ADD CONSTRAINT val_check CHECK (info.val > 0)")

      // Dropping top-level parent column 'info' should be blocked
      // because the constraint references 'info.val' which contains 'info'
      val e = intercept[Exception] {
        sql("ALTER TABLE T DROP COLUMN info")
      }
      Assertions.assertTrue(
        e.getMessage.contains("Cannot drop column") &&
          e.getMessage.contains("val_check"),
        s"Expected constraint reference error but got: ${e.getMessage}"
      )

      // Dropping unrelated column should succeed
      sql("ALTER TABLE T DROP COLUMN extra")
    }
  }
}
