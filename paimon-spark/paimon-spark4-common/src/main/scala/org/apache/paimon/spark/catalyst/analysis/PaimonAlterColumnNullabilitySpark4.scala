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

package org.apache.paimon.spark.catalyst.analysis

import org.apache.paimon.spark.{SparkCatalog, SparkGenericCatalog}
import org.apache.paimon.spark.commands.PaimonAlterColumnNullabilityCommand

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.ResolvedTable
import org.apache.spark.sql.catalyst.plans.logical.{AlterColumns, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Spark 4 specific rule to rewrite AlterColumns SET NOT NULL operations for Paimon tables. In Spark
 * 4, AlterColumn was renamed to AlterColumns with AlterColumnSpec support.
 *
 * This rule only matches when there is exactly one AlterColumnSpec and it is purely a SET NOT NULL
 * change (no data type, comment, position, or default expression changes). This avoids silently
 * dropping other column changes when multiple specs are present.
 */
case class PaimonAlterColumnNullabilitySpark4(session: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsDown {
    // Only match single-spec AlterColumns that is purely a SET NOT NULL change for Paimon tables
    case AlterColumns(table @ ResolvedTable(catalog, _, _, _), specs)
        if isPaimonCatalog(catalog)
          && specs.size == 1
          && specs.head.newNullability.contains(false)
          && specs.head.newDataType.isEmpty
          && specs.head.newComment.isEmpty
          && specs.head.newPosition.isEmpty
          && specs.head.newDefaultExpression.isEmpty =>
      PaimonAlterColumnNullabilityCommand(table, specs.head.column.name, nullable = false)
  }

  /** Check if the catalog is a Paimon catalog (SparkCatalog or SparkGenericCatalog). */
  private def isPaimonCatalog(catalog: Any): Boolean = {
    catalog match {
      case _: SparkCatalog => true
      case _: SparkGenericCatalog => true
      case _ => false
    }
  }
}
