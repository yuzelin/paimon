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

package org.apache.paimon.catalog;

import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FileSystemCatalog}. */
public class FileSystemCatalogTest extends CatalogTestBase {

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        catalog =
                new FileSystemCatalog(
                        fileIO, new Path(warehouse), CatalogContext.create(new Options()));
    }

    @Test
    public void testCreateTableCaseSensitive() throws Exception {
        catalog.createDatabase("test_db", false);
        Identifier identifier = Identifier.create("test_db", "new_TABLE");
        Schema schema =
                Schema.newBuilder()
                        .column("Pk1", DataTypes.INT())
                        .column("pk2", DataTypes.STRING())
                        .column("pk3", DataTypes.STRING())
                        .column(
                                "Col1",
                                DataTypes.ROW(
                                        DataTypes.STRING(),
                                        DataTypes.BIGINT(),
                                        DataTypes.TIMESTAMP(),
                                        DataTypes.ARRAY(DataTypes.STRING())))
                        .column("col2", DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT()))
                        .column("col3", DataTypes.ARRAY(DataTypes.ROW(DataTypes.STRING())))
                        .partitionKeys("Pk1", "pk2")
                        .primaryKey("Pk1", "pk2", "pk3")
                        .build();
        catalog.createTable(identifier, schema, false);
    }

    @Test
    public void testAlterDatabase() throws Exception {
        String databaseName = "test_alter_db";
        catalog.createDatabase(databaseName, false);
        assertThatThrownBy(
                        () ->
                                catalog.alterDatabase(
                                        databaseName,
                                        Lists.newArrayList(PropertyChange.removeProperty("a")),
                                        false))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    private Catalog createCatalogWithTrash() {
        Options options = new Options();
        options.set(CatalogOptions.TRASH_ENABLED, true);
        return new FileSystemCatalog(fileIO, new Path(warehouse), CatalogContext.create(options));
    }

    @Test
    public void testDropTableWithTrashEnabled() throws Exception {
        Catalog trashCatalog = createCatalogWithTrash();
        trashCatalog.createDatabase("test_db", false);

        Identifier identifier = Identifier.create("test_db", "table_to_trash");
        trashCatalog.createTable(identifier, DEFAULT_TABLE_SCHEMA, false);

        Path tablePath = ((FileSystemCatalog) trashCatalog).getTableLocation(identifier);
        assertThat(fileIO.exists(tablePath)).isTrue();

        trashCatalog.dropTable(identifier, false);

        // table should no longer exist at original location
        assertThat(fileIO.exists(tablePath)).isFalse();

        // table should be in trash
        Path trashDir = new Path(warehouse, ".trash");
        assertThat(fileIO.exists(trashDir)).isTrue();
        Path trashDbPath = new Path(trashDir, "test_db" + Catalog.DB_SUFFIX);
        Path trashTablePath = new Path(trashDbPath, "table_to_trash");
        assertThat(fileIO.exists(trashTablePath)).isTrue();

        trashCatalog.close();
    }

    @Test
    public void testDropTableWithTrashDisabled() throws Exception {
        catalog.createDatabase("test_db", false);

        Identifier identifier = Identifier.create("test_db", "table_no_trash");
        catalog.createTable(identifier, DEFAULT_TABLE_SCHEMA, false);

        Path tablePath = ((FileSystemCatalog) catalog).getTableLocation(identifier);
        assertThat(fileIO.exists(tablePath)).isTrue();

        catalog.dropTable(identifier, false);

        // table should be permanently deleted
        assertThat(fileIO.exists(tablePath)).isFalse();
        // no trash directory should exist
        Path trashDir = new Path(warehouse, ".trash");
        assertThat(fileIO.exists(trashDir)).isFalse();
    }

    @Test
    public void testDropTableWithTrashNameCollision() throws Exception {
        Catalog trashCatalog = createCatalogWithTrash();
        trashCatalog.createDatabase("test_db", false);

        Identifier identifier = Identifier.create("test_db", "collision_table");

        // drop the same-named table twice
        trashCatalog.createTable(identifier, DEFAULT_TABLE_SCHEMA, false);
        trashCatalog.dropTable(identifier, false);

        trashCatalog.createTable(identifier, DEFAULT_TABLE_SCHEMA, false);
        trashCatalog.dropTable(identifier, false);

        // both should exist in trash (second with epoch millis suffix)
        Path trashDir = new Path(warehouse, ".trash");
        Path trashDbPath = new Path(trashDir, "test_db" + Catalog.DB_SUFFIX);
        FileStatus[] trashEntries = fileIO.listStatus(trashDbPath);
        assertThat(trashEntries.length).isEqualTo(2);

        trashCatalog.close();
    }

    @Test
    public void testDropTableWithCustomTrashDir() throws Exception {
        String customTrashDir = tempFile.resolve("custom-trash").toUri().toString();
        Options options = new Options();
        options.set(CatalogOptions.TRASH_ENABLED, true);
        options.set(CatalogOptions.TRASH_DIR, customTrashDir);
        Catalog trashCatalog =
                new FileSystemCatalog(fileIO, new Path(warehouse), CatalogContext.create(options));
        trashCatalog.createDatabase("test_db", false);

        Identifier identifier = Identifier.create("test_db", "custom_trash_table");
        trashCatalog.createTable(identifier, DEFAULT_TABLE_SCHEMA, false);
        trashCatalog.dropTable(identifier, false);

        Path trashTablePath =
                new Path(
                        new Path(customTrashDir, "test_db" + Catalog.DB_SUFFIX),
                        "custom_trash_table");
        assertThat(fileIO.exists(trashTablePath)).isTrue();

        // default trash dir should not exist
        assertThat(fileIO.exists(new Path(warehouse, ".trash"))).isFalse();

        trashCatalog.close();
    }
}
