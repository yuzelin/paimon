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
import org.apache.spark.sql.catalyst.plans.logical.{AlterColumn, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Spark 3 specific rule to rewrite AlterColumn SET NOT NULL operations for Paimon tables. This
 * bypasses Spark's CheckAnalysis restriction that prevents changing nullable columns to
 * non-nullable.
 */
case class PaimonAlterColumnNullabilitySpark3(session: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsDown {
    // Match AlterColumn with nullable=Some(false) for Paimon tables (SET NOT NULL)
    case AlterColumn(table @ ResolvedTable(catalog, _, _, _), column, _, Some(false), _, _, _)
        if isPaimonCatalog(catalog) =>
      PaimonAlterColumnNullabilityCommand(table, column.name, nullable = false)
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
