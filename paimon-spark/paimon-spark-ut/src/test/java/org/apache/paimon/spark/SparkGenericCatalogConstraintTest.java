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

package org.apache.paimon.spark;

import org.apache.paimon.fs.Path;
import org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for NOT NULL and CHECK constraints when using {@link SparkGenericCatalog} as spark_catalog.
 */
public class SparkGenericCatalogConstraintTest {

    protected SparkSession spark = null;

    protected Path warehousePath = null;

    @BeforeEach
    public void startMetastoreAndSpark(@TempDir java.nio.file.Path tempDir) {
        warehousePath = new Path("file:" + tempDir.toString());
        spark =
                SparkSession.builder()
                        .config(
                                "spark.sql.catalog.spark_catalog",
                                SparkGenericCatalog.class.getName())
                        .config(
                                "spark.sql.extensions",
                                PaimonSparkSessionExtensions.class.getName())
                        .config("spark.sql.warehouse.dir", warehousePath.toString())
                        .master("local[2]")
                        .getOrCreate();
    }

    @AfterEach
    public void stopMetastoreAndSpark() {
        if (spark != null) {
            spark.stop();
            spark = null;
        }
    }

    @Test
    public void testSetAndDropNotNull() {
        spark.sql("CREATE TABLE T (id INT, name STRING, value INT) USING paimon");
        spark.sql("INSERT INTO T VALUES (1, 'Alice', 100)");

        // SET NOT NULL on 'value' column
        spark.sql("ALTER TABLE T ALTER COLUMN value SET NOT NULL");

        // Verify schema
        assertThat(spark.table("T").schema().apply("value").nullable()).isFalse();

        // Insert null should fail
        assertThatThrownBy(() -> spark.sql("INSERT INTO T VALUES (2, 'Bob', null)").collect())
                .hasStackTraceContaining("value appeared in non-nullable field");

        // DROP NOT NULL on 'value' column
        spark.sql("ALTER TABLE T ALTER COLUMN value DROP NOT NULL");

        // Verify schema changed back
        assertThat(spark.table("T").schema().apply("value").nullable()).isTrue();

        // Now null values can be inserted again
        spark.sql("INSERT INTO T VALUES (3, 'Charlie', null)");
        List<Row> rows = spark.sql("SELECT * FROM T ORDER BY id").collectAsList();
        assertThat(rows.stream().map(Object::toString))
                .containsExactly("[1,Alice,100]", "[3,Charlie,null]");

        spark.sql("DROP TABLE T");
    }

    @Test
    public void testSetNotNullValidatesExistingData() {
        spark.sql("CREATE TABLE T (id INT, value INT) USING paimon");
        spark.sql("INSERT INTO T VALUES (1, 100)");
        spark.sql("INSERT INTO T VALUES (2, null)");

        // SET NOT NULL should fail because existing data has NULL values
        assertThatThrownBy(
                        () -> spark.sql("ALTER TABLE T ALTER COLUMN value SET NOT NULL").collect())
                .hasMessageContaining("NULL values");

        // Column should still be nullable
        assertThat(spark.table("T").schema().apply("value").nullable()).isTrue();

        spark.sql("DROP TABLE T");
    }

    @Test
    public void testCreateTableWithNotNull() {
        spark.sql("CREATE TABLE T (id INT NOT NULL, name STRING) USING paimon");

        // Verify schema
        assertThat(spark.table("T").schema().apply("id").nullable()).isFalse();
        assertThat(spark.table("T").schema().apply("name").nullable()).isTrue();

        // Insert null for NOT NULL column should fail
        assertThatThrownBy(() -> spark.sql("INSERT INTO T VALUES (null, 'test')").collect())
                .hasStackTraceContaining("value appeared in non-nullable field");

        // Valid insert should succeed
        spark.sql("INSERT INTO T VALUES (1, 'Alice')");
        List<Row> rows = spark.sql("SELECT * FROM T").collectAsList();
        assertThat(rows.stream().map(Object::toString)).containsExactly("[1,Alice]");

        spark.sql("DROP TABLE T");
    }

    @Test
    public void testCheckConstraintWithGenericCatalog() {
        spark.sql("CREATE TABLE T (id INT, salary INT) USING paimon");
        spark.sql("ALTER TABLE T ADD CONSTRAINT salary_positive CHECK (salary > 0)");

        // Valid insert should succeed
        spark.sql("INSERT INTO T VALUES (1, 100)");

        // Invalid insert should fail
        assertThatThrownBy(() -> spark.sql("INSERT INTO T VALUES (2, -10)").collect())
                .hasMessageContaining("CHECK constraint");

        // Verify data
        List<Row> rows = spark.sql("SELECT * FROM T ORDER BY id").collectAsList();
        assertThat(rows.stream().map(Object::toString)).containsExactly("[1,100]");

        // Drop constraint and then insert should succeed
        spark.sql("ALTER TABLE T DROP CONSTRAINT salary_positive");
        spark.sql("INSERT INTO T VALUES (3, -10)");

        rows = spark.sql("SELECT * FROM T ORDER BY id").collectAsList();
        assertThat(rows.stream().map(Object::toString)).containsExactly("[1,100]", "[3,-10]");

        spark.sql("DROP TABLE T");
    }

    @Test
    public void testNotNullAndCheckConstraintTogether() {
        spark.sql("CREATE TABLE T (id INT NOT NULL, salary INT) USING paimon");
        spark.sql("ALTER TABLE T ADD CONSTRAINT salary_positive CHECK (salary > 0)");

        // Both constraints should be enforced
        // Null id should fail
        assertThatThrownBy(() -> spark.sql("INSERT INTO T VALUES (null, 100)").collect())
                .hasStackTraceContaining("value appeared in non-nullable field");

        // Negative salary should fail
        assertThatThrownBy(() -> spark.sql("INSERT INTO T VALUES (1, -10)").collect())
                .hasMessageContaining("CHECK constraint");

        // Valid insert
        spark.sql("INSERT INTO T VALUES (1, 100)");
        List<Row> rows = spark.sql("SELECT * FROM T").collectAsList();
        assertThat(rows.stream().map(Object::toString)).containsExactly("[1,100]");

        spark.sql("DROP TABLE T");
    }
}
