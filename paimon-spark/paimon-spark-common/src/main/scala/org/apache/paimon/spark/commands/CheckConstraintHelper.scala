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

import org.apache.paimon.schema.SchemaManager
import org.apache.paimon.table.FileStoreTable

import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._

/** Shared utility for CHECK constraint validation across all write paths. */
private[spark] trait CheckConstraintHelper {

  /**
   * Get all check constraints from the latest schema of the given table. Uses schemaManager to
   * ensure the most recent constraints are loaded even if the table object is cached.
   */
  protected def getCheckConstraints(table: FileStoreTable): Map[String, String] = {
    val prefix = SchemaManager.CHECK_CONSTRAINT_PREFIX
    val latestSchema = table.schemaManager().latest()
    val latestOptions =
      if (latestSchema.isPresent) latestSchema.get().options() else table.options()
    latestOptions.asScala
      .filter { case (key, _) => key.startsWith(prefix) }
      .map { case (key, value) => (key.substring(prefix.length), value) }
      .toMap
  }

  /**
   * Validate all check constraints on the given data. First performs a single combined scan to
   * detect any violation. Only if a violation is found, drills down to identify the specific
   * violated constraint for a precise error message.
   */
  protected def validateCheckConstraints(table: FileStoreTable, data: DataFrame): Unit = {
    val constraints = getCheckConstraints(table)
    if (constraints.isEmpty) {
      return
    }

    // Fast path: single combined scan to check if any constraint is violated
    val violationConditions = constraints.map {
      case (_, constraintExpr) =>
        s"(NOT ($constraintExpr) OR ($constraintExpr) IS NULL)"
    }
    val combinedFilter = violationConditions.mkString(" OR ")
    if (data.filter(combinedFilter).head(1).isEmpty) {
      return
    }

    // Slow path: cache data to ensure consistent evaluation, then identify the
    // specific violated constraint for a precise error message.
    val cachedData = data.cache()
    try {
      constraints.foreach {
        case (constraintName, constraintExpr) =>
          val violationFilter = s"NOT ($constraintExpr) OR ($constraintExpr) IS NULL"
          val violationCount = cachedData.filter(violationFilter).count()
          if (violationCount > 0) {
            throw new RuntimeException(
              s"CHECK constraint '$constraintName' ($constraintExpr) violated by $violationCount rows")
          }
      }
    } finally {
      cachedData.unpersist()
    }
  }
}
