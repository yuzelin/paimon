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

package org.apache.paimon.spark.commands

import org.apache.paimon.catalog.Catalog
import org.apache.paimon.spark.{SparkCatalog, SparkGenericCatalog}

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.ResolvedTable
import org.apache.spark.sql.execution.command.LeafRunnableCommand

/**
 * Command to alter column nullability for Paimon tables.
 *
 * This command bypasses Spark's CheckAnalysis restrictions that prevent changing nullable columns
 * to non-nullable.
 *
 * @param table
 *   The resolved table
 * @param fieldNames
 *   The field names (supports nested fields)
 * @param nullable
 *   The new nullability (false = SET NOT NULL, true = DROP NOT NULL)
 */
case class PaimonAlterColumnNullabilityCommand(
    table: ResolvedTable,
    fieldNames: Seq[String],
    nullable: Boolean)
  extends LeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val paimonCatalog = table.catalog match {
      case c: SparkCatalog => c
      case c: SparkGenericCatalog => c
      case other =>
        throw new UnsupportedOperationException(s"Catalog ${other.name()} is not a Paimon catalog")
    }

    // When setting NOT NULL (nullable = false), validate that no existing data has NULL values
    if (!nullable) {
      validateNoNullValues(sparkSession)
    }

    val schemaChange = org.apache.paimon.schema.SchemaChange.updateColumnNullability(
      fieldNames.toArray,
      nullable
    )

    val namespace = table.identifier.namespace()
    if (namespace.isEmpty) {
      throw new IllegalArgumentException(
        s"Table identifier ${table.identifier} has an empty namespace")
    }
    val identifier = new org.apache.paimon.catalog.Identifier(
      namespace.head,
      table.identifier.name()
    )

    try {
      val catalog = paimonCatalog.paimonCatalog()

      if (!nullable) {
        // Check whether the option was explicitly set before we modify it.
        val paimonTable = catalog.getTable(identifier)
        val hadExplicitOption =
          paimonTable.options().containsKey("alter-column-null-to-not-null.disabled")
        val originalValue = paimonTable
          .options()
          .getOrDefault("alter-column-null-to-not-null.disabled", "true")

        // When setting NOT NULL, we need to disable the Core-level nullability guard first.
        // SchemaManager reads 'alter-column-null-to-not-null.disabled' from oldOptions before
        // processing changes, so we must set the option in a separate alterTable call.
        catalog.alterTable(
          identifier,
          java.util.Collections.singletonList(
            org.apache.paimon.schema.SchemaChange
              .setOption("alter-column-null-to-not-null.disabled", "false")),
          false
        )

        var originalException: Throwable = null
        try {
          catalog.alterTable(
            identifier,
            java.util.Collections.singletonList(schemaChange),
            false
          )
        } catch {
          case e: Throwable =>
            originalException = e
            throw e
        } finally {
          // Always restore the original option state, whether the nullability change
          // succeeded or failed. This prevents permanently weakening the safety guard.
          val restoreChange = if (hadExplicitOption) {
            org.apache.paimon.schema.SchemaChange
              .setOption("alter-column-null-to-not-null.disabled", originalValue)
          } else {
            org.apache.paimon.schema.SchemaChange
              .removeOption("alter-column-null-to-not-null.disabled")
          }
          try {
            catalog.alterTable(
              identifier,
              java.util.Collections.singletonList(restoreChange),
              false
            )
          } catch {
            case restoreEx: Throwable =>
              if (originalException != null) {
                originalException.addSuppressed(restoreEx)
              } else {
                throw restoreEx
              }
          }
        }
      } else {
        catalog.alterTable(
          identifier,
          java.util.Collections.singletonList(schemaChange),
          false
        )
      }
    } catch {
      case e: Catalog.TableNotExistException =>
        throw new RuntimeException(s"Table $identifier does not exist", e)
      case e: Catalog.ColumnNotExistException =>
        throw new RuntimeException(s"Column ${fieldNames.mkString(".")} does not exist", e)
      case e: Exception =>
        throw new RuntimeException(s"Failed to alter column nullability: ${e.getMessage}", e)
    }

    Seq.empty[Row]
  }

  /**
   * Validate that the column has no NULL values before setting NOT NULL constraint. Throws an
   * exception if any NULL values exist.
   */
  private def validateNoNullValues(sparkSession: SparkSession): Unit = {
    val catalogName = table.catalog.name()
    val tableName =
      s"$catalogName.${table.identifier.namespace().mkString(".")}.${table.identifier.name()}"
    // Use backticks around each individual field name segment to correctly handle
    // nested fields (e.g., `struct_col`.`field`) instead of wrapping the entire
    // dotted name in a single pair of backticks.
    val columnRef = fieldNames.map(n => s"`$n`").mkString(".")

    val tableDF = sparkSession.table(tableName)
    val nullCount = tableDF.filter(s"$columnRef IS NULL").count()

    if (nullCount > 0) {
      throw new RuntimeException(
        s"Cannot set NOT NULL on column '${fieldNames.mkString(".")}': " +
          s"$nullCount existing rows have NULL values")
    }
  }
}
