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

package org.apache.paimon.schema;

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for CHECK constraint behavior with column rename and drop. */
public class ConstraintColumnRenameAndDropTest {

    @TempDir java.io.File tempPath;

    private List<DataField> createFields(Object... fieldDefs) {
        List<DataField> fields = new ArrayList<>();
        for (int i = 0; i < fieldDefs.length; i += 4) {
            int id = (Integer) fieldDefs[i];
            String name = (String) fieldDefs[i + 1];
            org.apache.paimon.types.DataType type =
                    (org.apache.paimon.types.DataType) fieldDefs[i + 2];
            boolean nullable = (Boolean) fieldDefs[i + 3];
            fields.add(new DataField(id, name, type.copy(nullable)));
        }
        return fields;
    }

    @Test
    public void testRenameColumnUpdatesCheckConstraintExpression() throws Exception {
        File tableDir = new File(tempPath, "table1");
        tableDir.mkdirs();
        Path tablePath = new Path(tableDir.getAbsolutePath());
        SchemaManager schemaManager = new SchemaManager(LocalFileIO.create(), tablePath);

        Schema schema =
                new Schema(
                        createFields(
                                0,
                                "id",
                                DataTypes.INT(),
                                false,
                                1,
                                "salary",
                                DataTypes.INT(),
                                true),
                        Collections.emptyList(),
                        Collections.singletonList("id"),
                        new HashMap<>(),
                        "test table");

        schemaManager.createTable(schema);
        schemaManager.commitChanges(
                SchemaChange.addCheckConstraint("salary_positive", "salary > 0"));

        // Rename column
        schemaManager.commitChanges(SchemaChange.renameColumn("salary", "income"));

        // Verify constraint expression was updated
        TableSchema updatedSchema = schemaManager.latest().get();
        assertThat(updatedSchema.options().get("constraint.check.salary_positive"))
                .isEqualTo("income > 0");
        assertThat(updatedSchema.fieldNames()).containsExactly("id", "income");
    }

    @Test
    public void testRenameColumnWithComplexExpression() throws Exception {
        File tableDir = new File(tempPath, "table2");
        tableDir.mkdirs();
        Path tablePath = new Path(tableDir.getAbsolutePath());
        SchemaManager schemaManager = new SchemaManager(LocalFileIO.create(), tablePath);

        Schema schema =
                new Schema(
                        createFields(
                                0,
                                "id",
                                DataTypes.INT(),
                                false,
                                1,
                                "age",
                                DataTypes.INT(),
                                true,
                                2,
                                "salary",
                                DataTypes.INT(),
                                true),
                        Collections.emptyList(),
                        Collections.singletonList("id"),
                        new HashMap<>(),
                        "test table");

        schemaManager.createTable(schema);
        schemaManager.commitChanges(
                SchemaChange.addCheckConstraint(
                        "complex_check", "age >= 18 AND salary > 0 AND age < 150"));

        // Rename 'age' column
        schemaManager.commitChanges(SchemaChange.renameColumn("age", "user_age"));

        // Verify constraint expression was updated
        TableSchema updatedSchema = schemaManager.latest().get();
        assertThat(updatedSchema.options().get("constraint.check.complex_check"))
                .isEqualTo("user_age >= 18 AND salary > 0 AND user_age < 150");
    }

    @Test
    public void testRenameColumnDoesNotAffectSimilarNames() throws Exception {
        File tableDir = new File(tempPath, "table3");
        tableDir.mkdirs();
        Path tablePath = new Path(tableDir.getAbsolutePath());
        SchemaManager schemaManager = new SchemaManager(LocalFileIO.create(), tablePath);

        Schema schema =
                new Schema(
                        createFields(
                                0,
                                "id",
                                DataTypes.INT(),
                                false,
                                1,
                                "age",
                                DataTypes.INT(),
                                true,
                                2,
                                "wage",
                                DataTypes.INT(),
                                true),
                        Collections.emptyList(),
                        Collections.singletonList("id"),
                        new HashMap<>(),
                        "test table");

        schemaManager.createTable(schema);
        schemaManager.commitChanges(
                SchemaChange.addCheckConstraint("wage_check", "wage > 0 AND age > 0"));

        // Rename 'age' to 'new_age' - should NOT affect 'wage'
        schemaManager.commitChanges(SchemaChange.renameColumn("age", "new_age"));

        TableSchema updatedSchema = schemaManager.latest().get();
        assertThat(updatedSchema.options().get("constraint.check.wage_check"))
                .isEqualTo("wage > 0 AND new_age > 0");
    }

    @Test
    public void testDropColumnReferencedByConstraintFails() throws Exception {
        File tableDir = new File(tempPath, "table4");
        tableDir.mkdirs();
        Path tablePath = new Path(tableDir.getAbsolutePath());
        SchemaManager schemaManager = new SchemaManager(LocalFileIO.create(), tablePath);

        Schema schema =
                new Schema(
                        createFields(
                                0,
                                "id",
                                DataTypes.INT(),
                                false,
                                1,
                                "value",
                                DataTypes.INT(),
                                true,
                                2,
                                "name",
                                DataTypes.STRING(),
                                true),
                        Collections.emptyList(),
                        Collections.singletonList("id"),
                        new HashMap<>(),
                        "test table");

        schemaManager.createTable(schema);
        schemaManager.commitChanges(SchemaChange.addCheckConstraint("value_check", "value > 0"));

        // Drop column referenced by constraint should fail
        assertThatThrownBy(() -> schemaManager.commitChanges(SchemaChange.dropColumn("value")))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Cannot drop column [value]")
                .hasMessageContaining("value_check");
    }

    @Test
    public void testDropColumnNotReferencedByConstraintSucceeds() throws Exception {
        File tableDir = new File(tempPath, "table5");
        tableDir.mkdirs();
        Path tablePath = new Path(tableDir.getAbsolutePath());
        SchemaManager schemaManager = new SchemaManager(LocalFileIO.create(), tablePath);

        Schema schema =
                new Schema(
                        createFields(
                                0,
                                "id",
                                DataTypes.INT(),
                                false,
                                1,
                                "value",
                                DataTypes.INT(),
                                true,
                                2,
                                "name",
                                DataTypes.STRING(),
                                true),
                        Collections.emptyList(),
                        Collections.singletonList("id"),
                        new HashMap<>(),
                        "test table");

        schemaManager.createTable(schema);
        schemaManager.commitChanges(SchemaChange.addCheckConstraint("value_check", "value > 0"));

        // Drop column NOT referenced by constraint should succeed
        schemaManager.commitChanges(SchemaChange.dropColumn("name"));

        TableSchema updatedSchema = schemaManager.latest().get();
        assertThat(updatedSchema.fieldNames()).containsExactly("id", "value");
        assertThat(updatedSchema.options()).containsKey("constraint.check.value_check");
    }

    @Test
    public void testDropConstraintThenDropColumnSucceeds() throws Exception {
        File tableDir = new File(tempPath, "table6");
        tableDir.mkdirs();
        Path tablePath = new Path(tableDir.getAbsolutePath());
        SchemaManager schemaManager = new SchemaManager(LocalFileIO.create(), tablePath);

        Schema schema =
                new Schema(
                        createFields(
                                0,
                                "id",
                                DataTypes.INT(),
                                false,
                                1,
                                "value",
                                DataTypes.INT(),
                                true,
                                2,
                                "name",
                                DataTypes.STRING(),
                                true),
                        Collections.emptyList(),
                        Collections.singletonList("id"),
                        new HashMap<>(),
                        "test table");

        schemaManager.createTable(schema);
        schemaManager.commitChanges(SchemaChange.addCheckConstraint("value_check", "value > 0"));

        // Drop constraint first, then drop column should succeed
        schemaManager.commitChanges(SchemaChange.dropCheckConstraint("value_check"));
        schemaManager.commitChanges(SchemaChange.dropColumn("value"));

        TableSchema updatedSchema = schemaManager.latest().get();
        assertThat(updatedSchema.fieldNames()).containsExactly("id", "name");
        assertThat(updatedSchema.options()).doesNotContainKey("constraint.check.value_check");
    }

    @Test
    public void testMultipleConstraintsReferencingSameColumn() throws Exception {
        File tableDir = new File(tempPath, "table7");
        tableDir.mkdirs();
        Path tablePath = new Path(tableDir.getAbsolutePath());
        SchemaManager schemaManager = new SchemaManager(LocalFileIO.create(), tablePath);

        Schema schema =
                new Schema(
                        createFields(
                                0, "id", DataTypes.INT(), false, 1, "value", DataTypes.INT(), true),
                        Collections.emptyList(),
                        Collections.singletonList("id"),
                        new HashMap<>(),
                        "test table");

        schemaManager.createTable(schema);
        schemaManager.commitChanges(SchemaChange.addCheckConstraint("value_positive", "value > 0"));
        schemaManager.commitChanges(SchemaChange.addCheckConstraint("value_small", "value < 1000"));

        // Drop column should fail - referenced by multiple constraints
        assertThatThrownBy(() -> schemaManager.commitChanges(SchemaChange.dropColumn("value")))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Cannot drop column [value]");

        // Rename should update both constraints
        schemaManager.commitChanges(SchemaChange.renameColumn("value", "amount"));
        TableSchema updated = schemaManager.latest().get();
        assertThat(updated.options().get("constraint.check.value_positive"))
                .isEqualTo("amount > 0");
        assertThat(updated.options().get("constraint.check.value_small"))
                .isEqualTo("amount < 1000");
    }

    @Test
    public void testRenameAndDropInSingleCommitFails() throws Exception {
        File tableDir = new File(tempPath, "table8");
        tableDir.mkdirs();
        Path tablePath = new Path(tableDir.getAbsolutePath());
        SchemaManager schemaManager = new SchemaManager(LocalFileIO.create(), tablePath);

        Schema schema =
                new Schema(
                        createFields(
                                0, "id", DataTypes.INT(), false, 1, "value", DataTypes.INT(), true),
                        Collections.emptyList(),
                        Collections.singletonList("id"),
                        new HashMap<>(),
                        "test table");

        schemaManager.createTable(schema);
        schemaManager.commitChanges(SchemaChange.addCheckConstraint("value_check", "value > 0"));

        // Rename then drop in same commit - drop should still check against original schema
        assertThatThrownBy(
                        () ->
                                schemaManager.commitChanges(
                                        Arrays.asList(
                                                SchemaChange.renameColumn("value", "new_value"),
                                                SchemaChange.dropColumn("new_value"))))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Cannot drop column");
    }
}
