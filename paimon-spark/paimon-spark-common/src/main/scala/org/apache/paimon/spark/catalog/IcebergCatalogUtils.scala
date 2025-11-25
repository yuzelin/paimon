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

package org.apache.paimon.spark.catalog

import org.apache.paimon.spark.util.OptionUtils

import org.apache.spark.SPARK_VERSION
import org.apache.spark.internal.Logging
import org.apache.spark.sql.PaimonSparkSession

import java.util.Locale

import scala.util.matching.Regex

object IcebergCatalogUtils extends Logging {

  def usingIceberg(isIceberg: Boolean): Boolean = {
    if (OptionUtils.icebergEnabled()) {
      if (isIceberg) {
        if (SPARK_VERSION < "3.5") {
          throw new UnsupportedOperationException(
            "Spark version must be 3.5 or higher when enable iceberg table.")
        }
      }
      isIceberg
    } else {
      false
    }
  }

  def buildIcebergCatalogName(catalogName: String): String = {
    s"${catalogName}_iceberg"
  }

  def transformerIcebergCommandIfNeed(sqlText: String): String = {
    val normalized = sqlText
      .toLowerCase(Locale.ROOT)
      .trim()
      .replaceAll("--.*?\\n", " ")
      .replaceAll("\\s+", " ")
      .replaceAll("/\\*.*?\\*/", " ")
      .replaceAll("`", "")
      .trim()

    // todo(omni): handle more command if need.
    val isCallProcedure = normalized.startsWith("call") && normalized.contains("system.")
    if (usingIceberg(isCallProcedure)) {
      val pattern: Regex = """(?i)(\bcall\b)\s+(?:([a-zA-Z_][a-zA-Z0-9_]*)\.)?system\.(.*)""".r
      val catalogManager = PaimonSparkSession.active.sessionState.catalogManager
      val toMatch = sqlText
        .trim()
        .replaceAll("--.*?\\n", " ")
        .replaceAll("\\s+", " ")
        .replaceAll("/\\*.*?\\*/", " ")
        .trim()
      toMatch match {
        case pattern(call, null, rest) =>
          catalogManager.currentCatalog match {
            case _: SparkBaseCatalog =>
              val icebergCatalogName = buildIcebergCatalogName(catalogManager.currentCatalog.name())
              s"$call $icebergCatalogName.system.$rest"
            case _ => sqlText
          }
        case pattern(call, namespace, rest) =>
          catalogManager.catalog(namespace) match {
            case _: SparkBaseCatalog =>
              val icebergCatalogName = buildIcebergCatalogName(namespace)
              s"$call $icebergCatalogName.system.$rest"
            case _ => sqlText
          }
        case _ => sqlText
      }
    } else {
      sqlText
    }
  }
}
