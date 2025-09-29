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

package org.apache.paimon.spark.lance

import org.apache.paimon.fs.Path
import org.apache.paimon.spark.PaimonHiveTestBase
import org.apache.paimon.table.lance.LanceTable

import org.junit.jupiter.api.Assertions

class PaimonLanceTest extends PaimonHiveTestBase {

  import testImplicits._

  test("Lance Table") {
    sql("""
          |CREATE TABLE lance_table (id INT, name STRING, pt STRING)
          |USING PAIMON
          |TBLPROPERTIES ('type'='lance-table');
          |""".stripMargin)
    val paimonTable = loadRawTable(hiveDbName, "lance_table")
    Assertions.assertTrue(paimonTable.isInstanceOf[LanceTable])

    val fileIO = paimonTable.fileIO()
    val lancePaimonTable = paimonTable.asInstanceOf[LanceTable]
    val paimonTablePath = lancePaimonTable.location();
    Assertions.assertTrue(fileIO.exists(new Path(paimonTablePath)));
    Assertions.assertTrue(fileIO.isDir(new Path(paimonTablePath, "schema")));

    val lanceTablePath = paimonTablePath + "-lance";
    Assertions.assertTrue(fileIO.exists(new Path(lanceTablePath)));
    Assertions.assertTrue(fileIO.isDir(new Path(lanceTablePath, "_transactions")));
    Assertions.assertTrue(fileIO.isDir(new Path(lanceTablePath, "_versions")));

    sql("""
          |INSERT INTO lance_table VALUES (1, "a", "2025"), (2, "b", "2025");
          |""".stripMargin)
    checkAnswer(
      sql("SELECT * FROM lance_table ORDER BY id"),
      Seq((1, "a", "2025"), (2, "b", "2025")).toDF()
    )

    sql("""
          |INSERT OVERWRITE lance_table VALUES (3, "c", "2025"), (4, "d", "2025");
          |""".stripMargin)
    checkAnswer(
      sql("SELECT * FROM lance_table ORDER BY id"),
      Seq((3, "c", "2025"), (4, "d", "2025")).toDF()
    )

    sql("DROP TABLE lance_table")
    Assertions.assertFalse(fileIO.exists(new Path(lanceTablePath)));
    Assertions.assertFalse(fileIO.exists(new Path(lanceTablePath)));
  }
}
