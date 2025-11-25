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

import org.apache.paimon.spark.function.FunctionResources.{testUDFJarPath, UDFExampleAdd2Class}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession

class OmniCatalogTest extends QueryTest with SharedSparkSession {

  val accessKey: String = System.getenv("AK")
  val accessValue: String = System.getenv("SK")

  val omni = "pre_xinyu_catalog"
  val omniIceberg = "pre_xinyu_catalog_iceberg"

  override protected def sparkConf: SparkConf = {
    assert(accessKey != null && accessValue != null)
    super.sparkConf
      // Set dlf25 paimon catalog
      .set(s"spark.sql.catalog.$omni", "org.apache.paimon.spark.SparkCatalog")
      .set(s"spark.sql.catalog.$omni.metastore", "rest")
      .set(s"spark.sql.catalog.$omni.uri", "http://pre1-test-bennett.aliyun-inc.com")
      .set(s"spark.sql.catalog.$omni.warehouse", "pre_xinyu_catalog")
      .set(s"spark.sql.catalog.$omni.token.provider", "dlf")
      .set(s"spark.sql.catalog.$omni.dlf.access-key-id", accessKey)
      .set(s"spark.sql.catalog.$omni.dlf.access-key-secret", accessValue)
      .set(s"spark.sql.catalog.$omni.dlf.region", "cn-hangzhou")
      .set(s"spark.sql.catalog.$omni.dlf.oss-endpoint", "oss-cn-hangzhou.aliyuncs.com")
      // Set dlf25 iceberg catalog
      .set(s"spark.sql.catalog.$omniIceberg", "org.apache.iceberg.spark.SparkCatalog")
      .set(s"spark.sql.catalog.$omniIceberg.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
      .set(s"spark.sql.catalog.$omniIceberg.uri", "http://pre1-test-bennett.aliyun-inc.com/iceberg")
      .set(s"spark.sql.catalog.$omniIceberg.warehouse", "pre_xinyu_catalog")
      .set(s"spark.sql.catalog.$omniIceberg.io-impl", "org.apache.iceberg.rest.DlfFileIO")
      .set(s"spark.sql.catalog.$omniIceberg.rest.auth.type", "sigv4")
      .set(s"spark.sql.catalog.$omniIceberg.rest.signing-region", "cn-hangzhou")
      .set(s"spark.sql.catalog.$omniIceberg.dlf.region", "cn-hangzhou")
      .set(s"spark.sql.catalog.$omniIceberg.dlf.oss-endpoint", "oss-cn-hangzhou.aliyuncs.com")
      .set(s"spark.sql.catalog.$omniIceberg.rest.signing-name", "DlfNext")
      .set(s"spark.sql.catalog.$omniIceberg.rest.sigv4-enabled", "true")
      .set(s"spark.sql.catalog.$omniIceberg.rest.access-key-id", accessKey)
      .set(s"spark.sql.catalog.$omniIceberg.rest.secret-access-key", accessValue)
//      .set(
//        s"spark.sql.catalog.$omniIceberg.client.credentials-provider",
//        "org.apache.iceberg.rest.credentials.DlfEcsTokenCredentialsProvider")
      // Other config
      .set(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions," +
          "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions"
      )
  }

  ignore("test omni catalog") {
    sql(s"USE $omni")
    sql("use default")
    withSQLConf("spark.paimon.iceberg.enabled" -> "true") {
      val tableNamePrefix = "e2e_test_omni_catalog_test_tbl"
      val paimonTable = s"${tableNamePrefix}_paimon"
      val icebergTable = s"${tableNamePrefix}_iceberg"
      sql(s"drop table if exists $paimonTable")
      sql(s"drop table if exists $icebergTable")
      withTable(paimonTable, icebergTable) {
        // ------------------------ paimon table ------------------------
        sql(s"create table $paimonTable (id int, name string) using paimon")
        var createTblStr = sql(s"show create table $paimonTable").collect().toSeq.toString()
        assert(createTblStr.contains("USING paimon"))
        sql(s"insert into $paimonTable values (1, 'a1')")
        checkAnswer(sql(s"select * from $paimonTable"), Row(1, "a1"))
        checkAnswer(sql(s"select * from $omni.default.$paimonTable"), Row(1, "a1"))
        sql(s"CALL sys.compact(table => 'default.$paimonTable')")
        sql(s"CALL $omni.sys.compact(table => 'default.$paimonTable')")

        // ------------------------ iceberg table ------------------------
        // create
        sql(s"create table $icebergTable (id int) using iceberg")
        createTblStr = sql(s"show create table $icebergTable").collect().toSeq.toString()
        assert(createTblStr.contains("USING iceberg"))
        // insert
        sql(s"insert into $icebergTable values (2)")
        // alter
        sql(s"alter table $icebergTable add columns (name string)")
        sql(s"insert into $icebergTable values (3, 'a2')")
        // select
        checkAnswer(
          sql(s"select * from $icebergTable order by id"),
          Seq(Row(2, null), Row(3, "a2"))
        )
        checkAnswer(
          sql(s"select * from $omni.default.$icebergTable order by id"),
          Seq(Row(2, null), Row(3, "a2"))
        )
        checkAnswer(
          sql(s"select * from $omniIceberg.default.$icebergTable order by id"),
          Seq(Row(2, null), Row(3, "a2"))
        )
        // todo(omni): support iceberg system tables if need.

        // ------------------------ procedure ------------------------
        sql(s"CALL system.rewrite_data_files(table => 'default.$icebergTable')")
        sql(s"CALL $omni.system.rewrite_data_files(table => 'default.$icebergTable')")
        sql(s"CALL $omniIceberg.system.rewrite_data_files(table => 'default.$icebergTable')")

        // ------------------------ show tables ------------------------
        checkAnswer(
          sql(s"show tables like '$tableNamePrefix*'").select("tableName"),
          Seq(Row(paimonTable), Row(icebergTable))
        )

        // ------------------------ view ------------------------
        val viewName = s"${tableNamePrefix}_view"
        sql(
          s"create or replace view $viewName as select * from $paimonTable union select * from $icebergTable")
        checkAnswer(
          sql(s"select * from $viewName"),
          Seq(Row(1, "a1"), Row(2, null), Row(3, "a2"))
        )
        checkAnswer(
          sql(s"show views like '$viewName*'").select("viewName"),
          Seq(Row(viewName))
        )

        // ------------------------ function ------------------------
        val function = s"${tableNamePrefix}_function"
        sql(s"""
               |create or replace function $function AS '$UDFExampleAdd2Class'
               |using jar '$testUDFJarPath'
               |""".stripMargin)
        checkAnswer(
          sql(s"select $function(3, 4)"),
          Seq(Row(7))
        )
      }
    }
  }

  ignore("test omni catalog more iceberg test") {
    sql(s"USE $omni")
    sql("use default")
    withSQLConf("spark.paimon.iceberg.enabled" -> "true") {
      val tableNamePrefix = "e2e_test_omni_catalog_test_tbl"
      val icebergTable = s"${tableNamePrefix}_iceberg"
      sql(s"drop table if exists $icebergTable")
      withTable(icebergTable) {

        // ------------------------ iceberg table ------------------------
        // create
        sql(s"create table $icebergTable (id int) using iceberg")
        val createTblStr = sql(s"show create table $icebergTable").collect().toSeq.toString()
        assert(createTblStr.contains("USING iceberg"))

        // ------------------------ procedure ------------------------
        sql(s"""
               |call system.rewrite_data_files(table =>
               |'default.$icebergTable')
               |""".stripMargin)
        sql(s"""
               |CALL
               | $omni.system.rewrite_data_files(table =>
               | 'default.$icebergTable')""".stripMargin)
        sql(s"CALL $omniIceberg.system.rewrite_data_files(table => 'default.$icebergTable')")
      }
    }
  }

  ignore("test omni catalog drop table") {
    sql(s"USE $omni")
    sql("use default")
    withSQLConf("spark.paimon.iceberg.enabled" -> "true") {
      val tableNamePrefix = "e2e_test_omni_catalog_test_tbl"
      val icebergTable = s"${tableNamePrefix}_iceberg"
      sql(s"drop table if exists $icebergTable")
      withTable(icebergTable) {

        // ------------------------ iceberg table ------------------------
        // create
        sql(s"create table $icebergTable (id int) using iceberg")
        val createTblStr = sql(s"show create table $icebergTable").collect().toSeq.toString()
        assert(createTblStr.contains("USING iceberg"))

        sql("drop table if exists " + icebergTable)
        sql(s"create table $icebergTable (id int) using iceberg")
        val createTblStr1 = sql(s"show create table $icebergTable").collect().toSeq.toString()
        assert(createTblStr1.contains("USING iceberg"))
      }
    }
  }
}
