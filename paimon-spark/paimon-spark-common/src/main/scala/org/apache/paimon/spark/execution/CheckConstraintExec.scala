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

package org.apache.paimon.spark.execution

import org.apache.paimon.schema.SchemaChange
import org.apache.paimon.spark.catalog.WithPaimonCatalog
import org.apache.paimon.spark.leafnode.PaimonLeafV2CommandExec

import org.apache.spark.sql.PaimonSparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.types.StructType

import java.util.Collections

/** Execution for ADD CHECK CONSTRAINT. */
case class AddCheckConstraintExec(
    catalog: TableCatalog,
    ident: Identifier,
    constraintName: String,
    expression: String)
  extends PaimonLeafV2CommandExec {

  override protected def run(): Seq[InternalRow] = {
    catalog match {
      case wpc: WithPaimonCatalog =>
        val spark = PaimonSparkSession.active
        val namespace = ident.namespace()
        if (namespace.isEmpty) {
          throw new IllegalArgumentException(s"Table identifier $ident has an empty namespace")
        }
        // Use full catalog-qualified name to avoid resolving against wrong catalog
        val tableName =
          s"${catalog.name()}.${namespace.mkString(".")}.${ident.name()}"
        val tableDF = spark.table(tableName)

        // Validate that the expression is deterministic
        validateDeterministic(tableDF.schema)

        // Validate existing data before adding the constraint
        validateExistingData(tableDF)

        // Use Paimon Catalog alterTable to properly sync metadata across all catalog types
        val paimonCatalog = wpc.paimonCatalog()
        val paimonIdent =
          new org.apache.paimon.catalog.Identifier(namespace.last, ident.name())
        val schemaChange = SchemaChange.addCheckConstraint(constraintName, expression)
        paimonCatalog.alterTable(paimonIdent, Collections.singletonList(schemaChange), false)
        catalog.invalidateTable(ident)
      case _ =>
        throw new UnsupportedOperationException(
          s"Cannot add check constraint to non-Paimon catalog: ${catalog.name()}")
    }
    Nil
  }

  /**
   * Validate that the constraint expression is deterministic. Non-deterministic expressions (e.g.,
   * rand(), uuid(), current_timestamp()) are not allowed in CHECK constraints because they would
   * produce inconsistent results across different evaluations.
   */
  private def validateDeterministic(schema: StructType): Unit = {
    val spark = PaimonSparkSession.active
    val parsed = spark.sessionState.sqlParser.parseExpression(expression)

    // Create a dummy plan to analyze the expression
    val attrs = schema.map {
      field =>
        org.apache.spark.sql.catalyst.expressions
          .AttributeReference(field.name, field.dataType, field.nullable)()
    }
    val analyzed = spark.sessionState.analyzer
      .execute(
        org.apache.spark.sql.catalyst.plans.logical.Project(
          Seq(org.apache.spark.sql.catalyst.expressions.Alias(parsed, "result")()),
          org.apache.spark.sql.catalyst.plans.logical.LocalRelation(attrs))
      )
      .expressions
      .head
      .children
      .head

    if (!analyzed.deterministic) {
      throw new UnsupportedOperationException(
        s"CHECK constraint expression must be deterministic, " +
          s"but '$expression' contains non-deterministic functions")
    }
  }

  /**
   * Validate that all existing data in the table satisfies the constraint. Throws an exception if
   * any row violates the constraint.
   */
  private def validateExistingData(tableDF: org.apache.spark.sql.DataFrame): Unit = {
    // Find rows that violate the constraint: NOT (expression) OR (expression) IS NULL
    val violatingRows = tableDF.filter(s"NOT ($expression) OR ($expression) IS NULL")
    val violationCount = violatingRows.count()

    if (violationCount > 0) {
      throw new RuntimeException(
        s"Cannot add CHECK constraint '$constraintName' ($expression): " +
          s"$violationCount existing rows violate the constraint")
    }
  }

  override def output: Seq[Attribute] = Nil
}

/** Execution for DROP CHECK CONSTRAINT. */
case class DropCheckConstraintExec(
    catalog: TableCatalog,
    ident: Identifier,
    constraintName: String,
    ifExists: Boolean)
  extends PaimonLeafV2CommandExec {

  override protected def run(): Seq[InternalRow] = {
    catalog match {
      case wpc: WithPaimonCatalog =>
        try {
          val namespace = ident.namespace()
          if (namespace.isEmpty) {
            throw new IllegalArgumentException(s"Table identifier $ident has an empty namespace")
          }
          // Use Paimon Catalog alterTable to properly sync metadata across all catalog types
          val paimonCatalog = wpc.paimonCatalog()
          val paimonIdent =
            new org.apache.paimon.catalog.Identifier(namespace.last, ident.name())
          val schemaChange = SchemaChange.dropCheckConstraint(constraintName)
          paimonCatalog.alterTable(paimonIdent, Collections.singletonList(schemaChange), false)
          catalog.invalidateTable(ident)
        } catch {
          case _: IllegalArgumentException if ifExists =>
          // Constraint does not exist and IF EXISTS is specified, ignore
          case e: Exception => throw e
        }
      case _ =>
        throw new UnsupportedOperationException(
          s"Cannot drop check constraint from non-Paimon catalog: ${catalog.name()}")
    }
    Nil
  }

  override def output: Seq[Attribute] = Nil
}
